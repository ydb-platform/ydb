UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map)

SRCS(
    dirty_map_ut.cpp
    host_status_ut.cpp
    inflight_info_ut.cpp
    range_locker_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map

    library/cpp/testing/unittest
)

END()
