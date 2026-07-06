UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(10)
ENDIF()

SRCS(
    read_only_pdisk.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
