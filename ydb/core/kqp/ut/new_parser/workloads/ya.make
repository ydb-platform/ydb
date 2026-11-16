# Direct AST-to-RBO tests use original SQL resources and synthetic catalog
# snapshots. No host/legacy translator is available to rescue unsupported SQL.
UNITTEST_FOR(ydb/core/kqp)

SRCS(kqp_workload_lowering_ut.cpp)

PEERDIR(
    library/cpp/resource
    ydb/core/kqp/host/new_parser/rbo
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

# Compile the original workload inputs into the test without rewriting them.
RESOURCE(
    ../../rbo/data/schema/clickbench.sql clickbench/schema.sql
    ../../rbo/data/yql-clickbench/q20.yql clickbench/q20.yql
    ../../rbo/data/yql-clickbench/q25.yql clickbench/q25.yql
    ../../rbo/data/yql-clickbench/q26.yql clickbench/q26.yql
    ../../rbo/data/yql-clickbench/q27.yql clickbench/q27.yql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(compilation)
