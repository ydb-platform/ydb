LIBRARY()

SRCS(
    app_context.cpp
    buffer_pool.cpp
    helpers.cpp
    range_allocator.cpp
    range_map.cpp
    request_generator.cpp
    test_runner.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/protos
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/diagnostics

    util
)

END()

RECURSE(
)
