UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map)

SRCS(
    block_field_serializer_ut.cpp
    ddisk_state_ut.cpp
    dirty_map_ut.cpp
    inflight_info_ut.cpp
    range_locker_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/testlib
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    library/cpp/testing/unittest
)

END()
