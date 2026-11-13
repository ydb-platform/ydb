UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    phantom_blobs.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/blobstorage/vdisk/chunk_keeper
    ydb/core/blobstorage/vdisk/synclog
)

END()
