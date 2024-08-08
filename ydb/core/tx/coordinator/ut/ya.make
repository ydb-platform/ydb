UNITTEST_FOR(ydb/core/tx/coordinator)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx
    ydb/public/api/grpc
)

YQL_LAST_ABI_VERSION()

SRCS(
    coordinator_ut.cpp
    coordinator_volatile_ut.cpp
)

END()
