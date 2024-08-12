UNITTEST_FOR(ydb/library/yql/providers/yt/provider)

SIZE(SMALL)

SRCS(
    yql_yt_dq_integration_ut.cpp
    yql_yt_epoch_ut.cpp
    yql_yt_cbo_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/schema
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/providers/yt/comp_nodes/llvm14
    ydb/library/yql/core/ut_common
    ydb/library/yql/ast
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/core/services
    ydb/library/yql/core
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/config
    ydb/library/yql/providers/config
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/result/provider
    ydb/library/yql/sql
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
