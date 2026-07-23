UNITTEST_FOR(ydb/core/kqp/opt/rbo)

SRCS(
    correlated_scalar_rules_ut.cpp
    limit_pushdown_rules_ut.cpp
    semantic_snapshot_exporter_ut.cpp
)

PEERDIR(
    ydb/core/fq/libs/result_formatter
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/host
    ydb/core/kqp/opt
    ydb/core/kqp/opt/rbo
    ydb/core/kqp/provider
    yql/essentials/core
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/public/decimal
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
