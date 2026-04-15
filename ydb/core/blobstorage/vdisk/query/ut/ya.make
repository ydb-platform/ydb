UNITTEST_FOR(ydb/core/blobstorage/vdisk/query)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/vdisk/huge
    ydb/core/protos
)

SRCS(
    query_spacetracker_ut.cpp
)

END()
