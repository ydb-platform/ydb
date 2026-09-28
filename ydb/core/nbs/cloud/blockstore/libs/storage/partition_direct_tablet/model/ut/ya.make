UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/model)

SRCS(
    touched_vchunks_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/model

    library/cpp/testing/unittest
)

END()
