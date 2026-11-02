UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(30)
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_generated_columns_ut.cpp
    generated_column_matrix_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
