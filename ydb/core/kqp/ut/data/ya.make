UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    ydb/core/kqp/ut/common
    ydb/core/testlib
    ydb/core/tx
    ydb/core/tx/datashard/ut_common
    ydb/public/sdk/cpp/src/client/types
)

YQL_LAST_ABI_VERSION()

SRCS(
    kqp_read_null_ut.cpp
)


END()
