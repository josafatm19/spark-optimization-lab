import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import functions as F
from src.session import get_spark
from src.utils import benchmark, partition_stats

DATASET_PATH = "../data/sample/"
SALT_FACTOR = 10

def load_data(spark):
    df = spark.read.parquet(DATASET_PATH)
    print(f"\nDataset: {df.count():,} registros")
    partition_stats(df, "input")
    return df

def bench_partition_pruning(df):
    print("\n-- Benchmark 1: Partition Pruning --")
    spark = df.sparkSession
    spark.sparkContext.setJobDescription("PRUNING-full-scan")
    t_full = benchmark(
        "full scan",
        lambda: df.count()
    )

    spark.sparkContext.setJobDescription("PRUNING-single-year")
    t_pruned = benchmark(
        "single year (partition pruning)",
        lambda: df.filter("year == 2022 & month == '02'").count()
    )

    improvement = round((t_full - t_pruned) / t_full * 100, 1)
    print(f"-> mejora: {improvement}% mas rapido con partition pruning")
    return {"full_scan:", t_full, "pruned:", "improvement_pct: ", improvement}

def bench_cache(df):
    print("\n-- Benchmark 2: Cache--")
    spark = df.sparkSession

    df_filtered = df.filter("rating == 5")

    # sin cach - dos queries sobre el mismo df
    spark.sparkContext.setJobDescription("CACHE-miss-1")
    t1 = benchmark("sin cache query 1", lambda: df_filtered.groupBy("year").count().collect())
    spark.sparkContext.setJobDescription("CACHE-miss-2")
    t2 = benchmark("sin cache query 2", lambda: df_filtered.groupBy("month").count().collect())
    t_no_cache = round(t1 + t2, 2)

    df_filtered.cache()
    df_filtered.count()

    spark.sparkContext.setJobDescription("CACHE-hit-1")
    t3 = benchmark("con cache query 1", lambda: df_filtered.groupBy("year").count().collect())
    spark.sparkContext.setJobDescription("CACHE-hit-2")
    t4 = benchmark("con cache query 2", lambda: df_filtered.groupBy("month").count().collect())

    t_cached = round(t3 + t4, 2)

    df_filtered.unpersist()

    improvement = round((t_no_cache - t_cached) / t_no_cache * 100, 1)
    print(f"Sin cache: {t_no_cache}s | con cache: {t_cached} | mejora: {improvement} %")
    return {"no cache": t_no_cache, "cached": t_cached, "improvement_pct": improvement}

def bench_skew(df):
    print("\nBenchmark 3: Data Skew + Salting")
    spark = df.sparkSession
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    df_enero = df.filter("month == 01")
    df_skewed = df.filter("month != 01")

    for _ in range(19):
        df_skewed = df_skewed.union(df_enero)
    
    df_skewed = df_skewed.repartition(8, "month")

    spark.sparkContext.setJobDescription("SKEW_sin_salting")
    t_skewed = benchmark(
        "sin salting",
        lambda: df_skewed.groupBy("month").agg(F.sum("helpful_vote")).collect()
    )

    # con salting
    spark.sparkContext.setJobDescription("SKEW_con_salting")
    t_salted = benchmark(
        "con salting",
        lambda: df_skewed
                .withColumn("salt", (F.rand() * SALT_FACTOR).cast("int"))
                .groupBy("month", "salt")
                .agg(F.sum("helpful_vote").alias("parcial"))
                .groupBy("month")
                .agg(F.sum("parcial"))
                .collect()
    )

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    improvement = round((t_skewed - t_salted) / t_skewed * 100, 1)
    print(f"-> sin salting: {t_skewed}s | con salting: {t_salted}s | mejora {improvement}%")
    return {"skewed": t_skewed, "salted": t_salted, "improvement_pct": improvement}

def bench_broadcast(df):
    print("\nBenchmark 4: broadcast join")
    spark = df.sparkSession
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    df_catalog = df.select("asin", "parent_asin").distinct().limit((10000))
    df_catalog.cache()
    df_catalog.count()

    spark.sparkContext.setJobDescription("JOIN-SHUFFLE")
    t_shuffle = benchmark(
        "shuffle_join,",
        lambda: df.join(df_catalog, "asin").count()
    )

    spark.sparkContext.setJobDescription("JOIN_BROADCAST")
    t_broadcast = benchmark(
        "broadcast join",
        lambda: df.join(F.broadcast(df_catalog), "asin").count()
    )

    df_catalog.unpersist()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

    improvement = round((t_shuffle - t_broadcast) / t_shuffle * 100, 1)
    print(f"→ shuffle: {t_shuffle}s | broadcast: {t_broadcast}s | mejora: {improvement}%")
    return {"shuffle": t_shuffle, "broadcast": t_broadcast, "improvement_pct": improvement}

def run_all():
    spark = get_spark("spark-optimization-lab")
    df = load_data(spark)
    results = {
        "partition_pruning": bench_partition_pruning(df),
        "cache":             bench_cache(df),
        "skew_salting":      bench_skew(df),
        "broadcast_join":    bench_broadcast(df),
    }

if __name__ == "__main__":
    run_all()