UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/synclog
    ydb/core/testlib/actors
    ydb/library/keys
    ydb/library/pdisk_io
)

SRCS(
    synclog_real_pdisk_ut.cpp
)

END()
