# Spark Fundamentals Homework — Bucket Joins, Broadcast Joins & Partition Tuning

PySpark job analyzing Halo match data (`matches`, `match_details`,
`medals_matches_players`, `medals`, `maps`) using Apache Iceberg tables.

## What it does

1. Disables automatic broadcast joins (`spark.sql.autoBroadcastJoinThreshold = -1`)
2. Bucket-joins `matches`, `match_details`, and `medals_matches_players` on
   `match_id` (16 buckets), using Iceberg's `bucket()` partition transform +
   Storage-Partitioned Joins (Spark 3.3+)
3. Explicitly broadcast-joins the small lookup tables `medals` and `maps`
4. Answers four questions from the joined dataset:
   - Which player averages the most kills per game?
   - Which playlist gets played the most?
   - Which map gets played the most?
   - Which map do players get the most Killing Spree medals on?
5. Compares four different partition/sort layouts to see which produces the
   smallest output size on disk

## Data

This project uses Halo 5 match data (`matches.csv`, `match_details.csv`,
`medals_matches_players.csv`, `medals.csv`, `maps.csv`) from the
DataExpert.io Spark Fundamentals bootcamp.

**Data files are not included in this repo** (see `.gitignore`), since they
aren't mine to redistribute and aren't needed to understand or review the
code itself. To reproduce the results, obtain the same five CSVs from the
DataExpert.io course materials and place them at `/home/iceberg/data/`
inside the Spark/Iceberg Docker container (see "Running it" below) — no
code changes are needed, since that path is defined by the course's Docker
setup rather than any machine-specific location.

## Key finding: shuffle-free joins with Iceberg + Storage-Partitioned Joins

Bucketing `match_details` and `matches` the same way (16 buckets on
`match_id`) and enabling:

```python
spark.conf.set("spark.sql.sources.v2.bucketing.enabled", "true")
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
```

lets Spark join them **without a shuffle** — confirmed via `explain()`, which
shows no `Exchange` node before either side of the join.

Joining in `medals_matches_players` on `match_id` **and** `player_gamertag`,
however, *does* still shuffle — even though all three tables are bucketed the
same way. This is expected: bucketing only guarantees colocation for exactly
the column(s) declared at bucket-creation time. A composite join key
(`match_id` + `player_gamertag`) isn't satisfied by a single-column bucket
spec (`match_id` only), regardless of how the query is written.

## Key finding: partition layout vs. output size

Four layouts were compared by writing `final_df` out each way and measuring
total output size:

| Layout | Strategy |
|---|---|
| version_a | `partitionBy("playlist_id")`, sorted within by `mapid` |
| version_b | `partitionBy("mapid")`, sorted within by `playlist_id` |
| version_c | `partitionBy("playlist_id", "mapid")`, sorted within by `match_id` |
| version_d | no partitioning, just `sortWithinPartitions("mapid")` (baseline) |

Results are printed at the end of the script run. In general, partitioning by
a low-cardinality column groups similar rows into the same files, which tends
to improve compression — but the exact winner depends on how evenly that
column splits the data.

## Data assumption on `final_df`

`final_df` has one row **per medal** (since it includes
`medals_matches_players`), not one row per player-per-match. Aggregations
that need a per-match or per-player granularity (average kills, match counts)
de-duplicate first to avoid over-weighting players/matches with many medals.
See comments in `q1_avg_kills_per_player`, `q2_most_played_playlist`, and
`q3_most_played_map` in the script.

## Running it

Requires a Spark environment with an Iceberg REST catalog configured (this
was developed against the `spark-iceberg` Docker container from the
DataExpert.io Spark Fundamentals course setup) and the five CSVs available at
`/home/iceberg/data/` (see "Data" above).

```bash
docker exec -it spark-iceberg spark-submit spark_join_optimization_analysis.py
```

Or paste the code into a Jupyter notebook cell-by-cell against the same
Spark session.
# spark-join-optimization-analysis-using-iceberg
