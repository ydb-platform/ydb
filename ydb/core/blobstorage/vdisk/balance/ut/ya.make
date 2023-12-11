UNITTEST_FOR(ydb/core/blobstorage/vdisk/balance)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/blobstorage/pdisk
    ydb/core/testlib/actors
)

SRCS(
    reader_ut.cpp
    ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_helpers.cpp
    ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_env.cpp
)

END()
