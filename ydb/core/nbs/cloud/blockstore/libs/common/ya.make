LIBRARY()

SRCS(
    printable_params.cpp
    thread_checker.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common/memory
    ydb/core/nbs/cloud/blockstore/libs/common/block_range
    ydb/core/nbs/cloud/storage/core/libs/coroutine
    library/cpp/lwtrace
    util
)

END()

RECURSE(
    block_range
    memory
)

RECURSE_FOR_TESTS(
    ut
)
