UNITTEST_FOR(ydb/core/keyvalue)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    keyvalue_ut_trace.cpp
)

END()
