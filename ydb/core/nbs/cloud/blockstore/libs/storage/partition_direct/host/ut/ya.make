UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host)

SRCS(
    ddisk_state_ut.cpp
    host_mask_ut.cpp
    host_status_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host

    library/cpp/testing/unittest
)

END()
