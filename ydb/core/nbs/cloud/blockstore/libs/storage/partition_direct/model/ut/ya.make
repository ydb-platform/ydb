UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model)

SRCS(
    host_mask_ut.cpp
    host_stat_ut.cpp
    host_status_ut.cpp
    oracle_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    library/cpp/testing/unittest
)

END()
