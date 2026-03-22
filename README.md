# Spark Optimization Lab

PySpark performance benchmarks on Amazon Reviews dataset (1.2M+ records).
Demonstrates core Spark internals through measurable optimizations.

## Optimizations covered

| Technique | Improvement |
|---|---|
| Partition pruning | 83% faster |
| Cache (multi-query) | 79% faster |
| Salting (skew 29x) | 17% faster |
| Broadcast join | 67% faster |

## Dataset

Amazon Customer Reviews — 1 year of data partitioned by year/month.
Format: Parquet. Size: ~540MB on disk, ~1.6M records.

## Setup
```bash
git clone https://github.com/TU_USUARIO/spark-optimization-lab.git
cd spark-optimization-lab
pip install -r requirements.txt
```

## Run benchmarks
```bash
PYTHONPATH=. python -m src.benchmarks
```

## Key findings

**Partition pruning** — filtering by partition column (year/month) skips
irrelevant files entirely. Spark reads only the target partitions.

**Cache** — caching a filtered DataFrame before running multiple aggregations
eliminates repeated disk reads. Effective when same DF is queried 2+ times.

**Salting** — artificially inflated January data (29x skew ratio) caused one
task to run 10x longer than others. Adding a random salt column distributed
load evenly across partitions.

**Broadcast join** — small catalog table (1k rows) broadcasted to all
executors eliminates shuffle entirely on the large side.

## Environment

- PySpark 3.5.0
- Python 3.x
- Local mode: 8 cores, 8GB driver memory