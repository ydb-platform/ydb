UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map)

SRCS(
    dirty_map_inflight_ut.cpp
    dirty_map_ut.cpp
    location_ut.cpp
    range_locker_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map

    library/cpp/testing/unittest
)

END()
