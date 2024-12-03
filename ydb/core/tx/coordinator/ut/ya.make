UNITTEST_FOR(ydb/core/tx/coordinator)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
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
