from .benchmark_abstract import BenchmarkAbstract
from .wiki_sql_dataset import WikiSqlBenchmark


def create_benchmark(name: str, config: dict) -> BenchmarkAbstract:
    if name == "wiki-sql":
        return WikiSqlBenchmark(config)
    raise ValueError(f"Unknown benchmark dataset: {name}")
