UNITTEST_FOR(ydb/core/kqp/opt/rbo)

SRCS(native_snapshot_ut.cpp)

PEERDIR(
    library/cpp/testing/common
    ydb/core/fq/libs/result_formatter
    ydb/core/kqp/common
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/host
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg
)

DEPENDS(
    contrib/tools/z3
    ydb/core/kqp/opt/rbo/verification/bin
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

YQL_LAST_ABI_VERSION()

END()
