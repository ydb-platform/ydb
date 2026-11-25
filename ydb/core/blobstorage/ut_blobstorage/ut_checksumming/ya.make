UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

SIZE(MEDIUM)

SRCS(
    checksumming.cpp
)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb/base
)

END()
