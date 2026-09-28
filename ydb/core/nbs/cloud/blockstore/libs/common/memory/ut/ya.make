UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/common/memory)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    arena_allocator_ut.cpp
    arena_allocator_index_pool_ut.cpp
    arena_allocator_pool_ut.cpp
    arena_std_containers_ut.cpp
)

PEERDIR(
)

END()
