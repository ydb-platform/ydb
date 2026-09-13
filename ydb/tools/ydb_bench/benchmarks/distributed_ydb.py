"""The local workload/search contract executed on a fixed multi-host cluster."""

from dataclasses import replace

from ydb.tools.ydb_bench.benchmarks.local_ydb import LOCAL_YDB_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.registry import BENCHMARKS

DISTRIBUTED_YDB_BENCHMARK = BENCHMARKS.register(
    replace(
        LOCAL_YDB_BENCHMARK,
        name="distributed-ydb",
        description="fixed multi-host YDB cluster driven by one YDB CLI generator",
        profile_kind="distributed-ydb",
        executor="distributed-ydb",
        builder_supported=True,
    )
)
