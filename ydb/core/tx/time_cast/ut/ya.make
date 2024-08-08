UNITTEST_FOR(ydb/core/tx/time_cast)

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
)

YQL_LAST_ABI_VERSION()

SRCS(
    time_cast_ut.cpp
)

END()
