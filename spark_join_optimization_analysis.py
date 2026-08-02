"""
Apache Spark Homework - Spark Fundamentals Week (Assignment 3)

Data: match_details, matches, medals_matches_players, medals, maps

What this script does:
  1. Disables automatic broadcast joins
  2. Bucket-joins match_details, matches, medals_matches_players on match_id (16 buckets),
     using Iceberg's bucket partition transform + Storage-Partitioned Joins (Spark 3.3+),
     which is a modern, officially supported shuffle-elimination technique -- confirmed
     working via explain("formatted") below (no Exchange node on the match_id-only join).
  3. Explicitly broadcast-joins the small lookup tables medals and maps into final_df
  4. Answers, using final_df (the fully joined dataset), as required:
       - Which player averages the most kills per game?
       - Which playlist gets played the most?
       - Which map gets played the most?
       - Which map do players get the most Killing Spree medals on?
  5. Compares 4 different partition/sort layouts to see which produces the smallest
     output size on disk

Important note on querying final_df directly:
final_df has one row PER MEDAL, because it's the result of joining in
medals_matches_players (one row per medal a player earned in a match). This means
a player's match_details row (kills, deaths) gets repeated once per medal they
earned in that match. If we naively grouped on final_df without accounting for
this, players who earn lots of medals would have their kill counts overweighted
in an average. Q1-Q3 below first de-duplicate down to one row per
(match_id, player_gamertag) [or per match_id, for match/playlist counts] before
aggregating, to avoid this fan-out bias. Q4 does not need this fix, since summing
medal counts per map is exactly what we want at medal-row granularity.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, countDistinct, sum as spark_sum, desc


DATA_PATH = "/home/iceberg/data"
NAMESPACE = "spark_assignment"


def load_data(spark):
    matches = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{DATA_PATH}/matches.csv")
    match_details = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{DATA_PATH}/match_details.csv")
    medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{DATA_PATH}/medals_matches_players.csv")
    medals = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{DATA_PATH}/medals.csv")
    maps = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv(f"{DATA_PATH}/maps.csv")
    return matches, match_details, medals_matches_players, medals, maps


def create_bucketed_tables(spark, matches, match_details, medals_matches_players):
    """Write the three big tables out as Iceberg tables bucketed by match_id
    (16 buckets), so joins on match_id alone can skip a shuffle via Storage-
    Partitioned Joins."""

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")

    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.matches_bucketed")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {NAMESPACE}.matches_bucketed (
            match_id STRING,
            mapid STRING,
            playlist_id STRING,
            completion_date TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket(16, match_id))
    """)
    matches.select("match_id", "mapid", "playlist_id", "completion_date") \
        .write.mode("append") \
        .saveAsTable(f"{NAMESPACE}.matches_bucketed")

    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.match_details_bucketed")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {NAMESPACE}.match_details_bucketed (
            match_id STRING,
            player_gamertag STRING,
            player_total_kills INT,
            player_total_deaths INT
        )
        USING iceberg
        PARTITIONED BY (bucket(16, match_id))
    """)
    match_details.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
        .write.mode("append") \
        .saveAsTable(f"{NAMESPACE}.match_details_bucketed")

    spark.sql(f"DROP TABLE IF EXISTS {NAMESPACE}.medals_matches_players_bucketed")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {NAMESPACE}.medals_matches_players_bucketed (
            match_id STRING,
            player_gamertag STRING,
            medal_id BIGINT,
            count INT
        )
        USING iceberg
        PARTITIONED BY (bucket(16, match_id))
    """)
    medals_matches_players.select("match_id", "player_gamertag", "medal_id", "count") \
        .write.mode("append") \
        .saveAsTable(f"{NAMESPACE}.medals_matches_players_bucketed")


def do_bucket_join(spark, verify_shuffle_free=True):
    """Join the three bucketed tables. The first join (match_details <-> matches)
    is on match_id alone, matching our bucket key, so it's shuffle-free thanks to
    Storage-Partitioned Joins. The second join (bringing in medals_matches_players)
    needs match_id + player_gamertag, so it still shuffles -- a single-column
    bucket key can't satisfy a composite join key, no matter how the query is
    written. This is expected, not a bug."""

    matches_b = spark.table(f"{NAMESPACE}.matches_bucketed")
    match_details_b = spark.table(f"{NAMESPACE}.match_details_bucketed")
    medals_mp_b = spark.table(f"{NAMESPACE}.medals_matches_players_bucketed")

    step1 = match_details_b.join(matches_b, "match_id")

    if verify_shuffle_free:
        print("\n=== Verifying match_id-only join is shuffle-free (no Exchange node expected) ===")
        step1.explain("formatted")

    joined = step1.join(medals_mp_b, ["match_id", "player_gamertag"])
    return joined


def do_broadcast_join(joined_df, medals, maps):
    """Broadcast the small lookup tables (medals, maps) into the joined dataset.
    Columns are renamed first to avoid duplicate "name"/"description" columns.
    This is the single place maps/medals get broadcast in the whole script --
    Q1-Q4 all read from this same final_df rather than re-broadcasting."""

    medals_renamed = medals.withColumnRenamed("name", "medal_name") \
                            .withColumnRenamed("description", "medal_description")
    maps_renamed = maps.withColumnRenamed("name", "map_name") \
                        .withColumnRenamed("description", "map_description")

    return joined_df.join(broadcast(medals_renamed), "medal_id", "left") \
                     .join(broadcast(maps_renamed), "mapid", "left")


def q1_avg_kills_per_player(final_df):
    """Which player averages the most kills per game?
    De-duplicated to one row per (match_id, player_gamertag) first, so players
    with many medals in a match don't get their kills counted multiple times."""
    per_match_player = final_df.dropDuplicates(["match_id", "player_gamertag", "player_total_kills"])
    return per_match_player.groupBy("player_gamertag") \
        .agg(avg("player_total_kills").alias("avg_kills")) \
        .orderBy(desc("avg_kills"))


def q2_most_played_playlist(final_df):
    """Which playlist gets played the most?
    countDistinct(match_id) is used instead of count(), since final_df has
    multiple rows per match (one per medal)."""
    return final_df.groupBy("playlist_id") \
        .agg(countDistinct("match_id").alias("num_matches")) \
        .orderBy(desc("num_matches"))


def q3_most_played_map(final_df):
    """Which map gets played the most?
    Same countDistinct(match_id) reasoning as Q2."""
    return final_df.groupBy("mapid", "map_name") \
        .agg(countDistinct("match_id").alias("num_matches")) \
        .orderBy(desc("num_matches"))


def q4_most_killing_spree_map(final_df):
    """Which map do players get the most Killing Spree medals on?
    No de-duplication needed here: each row in final_df already represents a
    unique (match, player, medal) combination, and count is the number of times
    that specific medal was earned in that match -- exactly what we want to sum."""
    return final_df.filter(final_df.medal_name == "Killing Spree") \
        .groupBy("mapid", "map_name") \
        .agg(spark_sum("count").alias("total_killing_sprees")) \
        .orderBy(desc("total_killing_sprees"))


def get_dir_size_mb(path):
    """Recursively sum file sizes under a directory, in megabytes.
    Used to compare output sizes without relying on a shell/subprocess call."""
    total_bytes = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_bytes += os.path.getsize(fp)
    return round(total_bytes / (1024 * 1024), 2)


def compare_partition_layouts(final_df):
    """Write 4 different partition/sort layouts and compare their output sizes.
    Low-cardinality columns (playlist_id, mapid) are used for partitioning and
    sorting, per the assignment's hint, since fewer distinct values should
    produce better grouping/compression."""

    # version_a: partition by playlist_id, sort within each partition by mapid
    final_df.sortWithinPartitions("mapid") \
        .write.mode("overwrite") \
        .partitionBy("playlist_id") \
        .parquet(f"{DATA_PATH}/output/version_a_partition_playlist_sort_map")

    # version_b: partition by mapid, sort within each partition by playlist_id
    final_df.sortWithinPartitions("playlist_id") \
        .write.mode("overwrite") \
        .partitionBy("mapid") \
        .parquet(f"{DATA_PATH}/output/version_b_partition_map_sort_playlist")

    # version_c: partition by both playlist_id and mapid, sort within by match_id
    final_df.sortWithinPartitions("match_id") \
        .write.mode("overwrite") \
        .partitionBy("playlist_id", "mapid") \
        .parquet(f"{DATA_PATH}/output/version_c_partition_playlist_map_sort_match")

    # version_d: baseline, no partitionBy at all, just a global sortWithinPartitions
    final_df.repartition(16).sortWithinPartitions("mapid") \
        .write.mode("overwrite") \
        .parquet(f"{DATA_PATH}/output/version_d_baseline_sort_only")

    print("\n=== Output size comparison ===")
    results = {}
    for name in [
        "version_a_partition_playlist_sort_map",
        "version_b_partition_map_sort_playlist",
        "version_c_partition_playlist_map_sort_match",
        "version_d_baseline_sort_only",
    ]:
        size_mb = get_dir_size_mb(f"{DATA_PATH}/output/{name}")
        results[name] = size_mb
        print(f"{name}: {size_mb} MB")

    smallest = min(results, key=results.get)
    print(f"\nSmallest output: {smallest} ({results[smallest]} MB)")
    print("Conclusion: partitioning by a low-cardinality column groups similar rows into")
    print("the same files, which tends to improve compression. Which specific column wins")
    print("depends on how evenly it splits the data and how well it clusters correlated")
    print("columns (e.g. mapid tends to correlate with playlist_id in this dataset).")


def main():
    spark = SparkSession.builder \
        .appName("spark_assignment_3") \
        .getOrCreate()

    # Disable automatic broadcast joins so we control broadcasting explicitly
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Enable storage-partitioned joins so bucketed tables can actually skip
    # the shuffle when the join key matches the bucket key
    spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
    spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")

    matches, match_details, medals_matches_players, medals, maps = load_data(spark)

    create_bucketed_tables(spark, matches, match_details, medals_matches_players)

    joined = do_bucket_join(spark)
    final_df = do_broadcast_join(joined, medals, maps)
    final_df.cache()  # reused across all 4 questions + the partitioning comparison

    print("\n=== Q1: Average kills per player (top 10), from final_df ===")
    q1_avg_kills_per_player(final_df).show(10)

    print("\n=== Q2: Most played playlist, from final_df ===")
    q2_most_played_playlist(final_df).show(10)

    print("\n=== Q3: Most played map, from final_df ===")
    q3_most_played_map(final_df).show(10)

    print("\n=== Q4: Map with most Killing Spree medals, from final_df ===")
    q4_most_killing_spree_map(final_df).show(10)

    print("\n=== Comparing 4 partition/sort layouts ===")
    compare_partition_layouts(final_df)


if __name__ == "__main__":
    main()
