from pyspark.sql import SparkSession

def get_spark(app_name: str = "spark-optimization-lab") -> SparkSession:
    return (
        SparkSession
        .builder
        .appName(app_name)
        .master("local[8]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )