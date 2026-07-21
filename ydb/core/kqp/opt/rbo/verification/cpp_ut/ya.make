UNITTEST_FOR(ydb/core/kqp/opt/rbo)

SRCS(
    semantic_snapshot_exporter_ut.cpp
)

PEERDIR(
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/opt
    ydb/core/kqp/opt/rbo
    ydb/core/kqp/provider
    yql/essentials/core
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
