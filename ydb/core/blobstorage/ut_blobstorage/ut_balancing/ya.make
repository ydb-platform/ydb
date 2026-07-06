UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(cpu:4)
    SPLIT_FACTOR(12)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
ENDIF()

FORK_SUBTESTS()

SRCS(
    balancing.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
