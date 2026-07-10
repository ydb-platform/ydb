UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()
SPLIT_FACTOR(12)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

SRCS(
    balancing.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
