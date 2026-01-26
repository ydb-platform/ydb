LIBRARY()

SRCS(
    kqp_explain_prepared.cpp
    kqp_gateway_proxy.cpp
    kqp_host.cpp
    kqp_runner.cpp
    kqp_transform.cpp
    kqp_translate.cpp
    kqp_statement_rewrite.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/federated_query
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    ydb/core/tx/long_tx_service/public
    ydb/library/yql/dq/opt
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/dq/helper
    ydb/library/yql/providers/generic/provider
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/s3/expr_nodes
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/minikql/invoke_builtins
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/udf_resolve
    yql/essentials/providers/config
    yql/essentials/providers/pg/provider
    yql/essentials/providers/result/provider
    yql/essentials/sql
    yql/essentials/sql/v0
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
