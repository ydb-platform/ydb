UNITTEST_FOR(ydb/core/tx/time_cast)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx
)

YQL_LAST_ABI_VERSION()

SRCS(
    time_cast_ut.cpp
)

END()
