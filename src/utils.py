import time
from typing import Callable
from pyspark.sql import DataFrame

def benchmark(description: str, func: Callable) -> float:
    """
    Ejecuta una funcion y retorna el tiempo en segundos
    """
    start = time.time()
    result = func()
    elapsed = round(time.time() - start, 2)
    print(f"[{description} {elapsed} s]")
    return elapsed

def partition_stats(df: DataFrame, label: str = "") -> None:
    """
    Imprime estadisticas de particionamiento de un dataframe
    """

    num_parts = df.rdd.getNumPartitions()
    print(f"[{label} particiones: {num_parts}]")