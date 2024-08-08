UNITTEST()

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(24)

IF (SANITIZER_TYPE OR WITH_VALGRIND OR BUILD_TYPE == "DEBUG")
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/ut_vdisk/lib
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    dsproxy_fault_tolerance_ut.cpp
)

END()
