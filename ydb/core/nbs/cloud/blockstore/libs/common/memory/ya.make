LIBRARY()

SRCS(
    arena_allocator.cpp
    arena_allocator_index_pool.cpp
    arena_allocator_pool.cpp
)

PEERDIR(
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
