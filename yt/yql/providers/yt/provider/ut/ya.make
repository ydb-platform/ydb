IF (NOT OPENSOURCE)

UNITTEST_FOR(yt/yql/providers/yt/provider)

SIZE(SMALL)

SRCS(
    yql_yt_dq_integration_ut.cpp
    yql_yt_epoch_ut.cpp
    yql_yt_cbo_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
    yt/yql/providers/yt/lib/schema
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/lib/ut_common
    yql/essentials/ast
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/core/services
    yql/essentials/core
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/config
    yql/essentials/providers/config
    yql/essentials/providers/result/provider
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

