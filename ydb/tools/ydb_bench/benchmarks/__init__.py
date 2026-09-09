"""Benchmark adapters and the registry used by ydb_bench."""

from ydb.tools.ydb_bench.benchmarks.actors import PING_BENCHMARK, STAR_PING_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.local_ydb import LOCAL_YDB_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.memory import MEMORY_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.registry import BENCHMARKS, BenchmarkRegistry

__all__ = (
    "BENCHMARKS",
    "BenchmarkRegistry",
    "PING_BENCHMARK",
    "STAR_PING_BENCHMARK",
    "MEMORY_BENCHMARK",
    "LOCAL_YDB_BENCHMARK",
)
