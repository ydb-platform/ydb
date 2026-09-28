UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/common/block_range)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
)

SRCS(
    block_range_algorithms_ut.cpp
    #block_range_field_flat_set_ut.cpp
    block_range_field_bitmask_ut.cpp
    block_range_field_set_fuzz_ut.cpp
    block_range_field_set_ut.cpp
    block_range_field_simple_ut.cpp
    block_range_field_ut.cpp
    block_range_map_ut.cpp
    block_range_ut.cpp
)

END()
