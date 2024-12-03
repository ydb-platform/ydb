LIBRARY()

SRCS(
    kqp_explain_prepared.cpp
    kqp_gateway_proxy.cpp
    kqp_host.cpp
    kqp_runner.cpp
    kqp_transform.cpp
    kqp_translate.cpp
    kqp_type_ann.cpp
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
    yql/essentials/core/services
    yql/essentials/minikql/invoke_builtins
    yql/essentials/sql
    yql/essentials/core
    yql/essentials/providers/common/codec
    ydb/library/yql/dq/opt
    ydb/library/yql/providers/dq/helper
    ydb/library/yql/providers/common/http_gateway
    yql/essentials/providers/common/udf_resolve
    yql/essentials/providers/config
    ydb/library/yql/providers/generic/provider
    yql/essentials/providers/pg/provider
    yql/essentials/providers/result/provider
    ydb/library/yql/providers/s3/expr_nodes
    ydb/public/sdk/cpp/client/impl/ydb_internal/common
)

YQL_LAST_ABI_VERSION()

END()
