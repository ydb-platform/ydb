UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(12)
ENDIF()

SRCS(
    restart_pdisk.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
