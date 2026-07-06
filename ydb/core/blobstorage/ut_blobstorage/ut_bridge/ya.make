UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    SPLIT_FACTOR(11)
ENDIF()

FORK_SUBTESTS()

SRCS(
    bridge_get.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
