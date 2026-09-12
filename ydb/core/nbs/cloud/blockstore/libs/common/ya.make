LIBRARY()

SRCS(
<<<<<<< HEAD
    block_range_algorithms.cpp
    block_range_map.cpp
    block_range.cpp
=======
    printable_params.cpp
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
