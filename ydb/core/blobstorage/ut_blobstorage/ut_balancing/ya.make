UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
    SPLIT_FACTOR(12)
ENDIF()

FORK_SUBTESTS()

SRCS(
    balancing.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
