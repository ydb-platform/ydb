G_BENCHMARK(nbs_block_range_field_benchmark)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

# Keeps the benchmark inside the SMALL test budget. Run the binary directly
# without this option when comparing performance numbers.
BENCHMARK_OPTS(--benchmark_min_time=0.05s)

SRCS(
    block_range_field_benchmark.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/common/block_range
)

END()
