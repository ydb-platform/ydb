UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/common)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    block_range_map_ut.cpp
    block_range_ut.cpp
)

PEERDIR(
)

END()
