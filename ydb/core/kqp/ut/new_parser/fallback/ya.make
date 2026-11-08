UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_new_parser_fallback_ut.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/kqp/ut/common
    ydb/library/benchmarks/queries/tpch
    ydb/library/yql/udfs/statistics_internal
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/langver
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/url_base
)

DATA(
    arcadia/ydb/core/kqp/ut/join/data
    arcadia/ydb/core/kqp/ut/rbo/data
)

RESOURCE(
    ydb/library/benchmarks/gen_queries/consts.yql consts.yql
)

YQL_LAST_ABI_VERSION()

END()
