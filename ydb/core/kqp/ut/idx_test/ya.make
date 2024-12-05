UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_index_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/public/lib/idx_test
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
