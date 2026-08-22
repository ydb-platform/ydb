UNITTEST_FOR(ydb/core/blobstorage/rocksdisk)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/util/actorsys_test
    ydb/core/blobstorage/rocksdisk
)

SRCS(
    rocksdisk_ut.cpp
)

END()
