G_BENCHMARK(nbs_partition_direct_requests_creation_benchmark)

SIZE(SMALL)

# Keeps the case inside the SMALL test budget. Run the binary directly without
# this option (or with a larger --benchmark_min_time) when comparing numbers.
BENCHMARK_OPTS(--benchmark_min_time=0.05s)

SRCS(
    requests_benchmark.cpp
    time_predictor_benchmark.cpp
    ../base_test_fixture.cpp
    ../direct_block_group_mock.cpp
    ../write_request_test_fixture.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib
    ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    ydb/core/protos
    ydb/core/testlib
)

END()
