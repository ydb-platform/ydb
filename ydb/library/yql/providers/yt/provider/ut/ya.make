IF (NOT OPENSOURCE)

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
    yql/essentials/core/ut_common
    yql/essentials/ast
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/core/services
    yql/essentials/core
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/config
    yql/essentials/providers/config
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/provider
    yql/essentials/providers/result/provider
    yql/essentials/sql
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()
