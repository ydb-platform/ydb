FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
    ../../blobstorage_pdisk_ut_context.cpp
    ../../blobstorage_pdisk_ut_env.cpp
    ../../blobstorage_pdisk_ut_helpers.cpp
)

PEERDIR(
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/pdisk/mock
    ydb/core/testlib/actors
    ydb/library/keys
    ydb/library/actors/wilson/test_util
    library/cpp/testing/unittest
)

END()
