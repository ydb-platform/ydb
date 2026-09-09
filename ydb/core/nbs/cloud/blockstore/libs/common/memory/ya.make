LIBRARY()

SRCS(
    arena_allocator.cpp
    arena_allocator_index_pool.cpp
    arena_allocator_pool.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/coroutine
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)