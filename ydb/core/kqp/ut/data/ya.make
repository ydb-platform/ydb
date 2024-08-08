UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/datashard/ut_common
    ydb/public/sdk/cpp/client/ydb_types
)

YQL_LAST_ABI_VERSION()

SRCS(
    kqp_read_null_ut.cpp
)


END()
