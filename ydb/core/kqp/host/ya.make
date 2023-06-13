LIBRARY()

SRCS(
    kqp_explain_prepared.cpp
    kqp_host.cpp
    kqp_runner.cpp
    kqp_transform.cpp
    kqp_type_ann.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    ydb/core/tx/long_tx_service/public
    ydb/library/yql/ast
    ydb/library/yql/core/services
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/sql
    ydb/library/yql/core
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/config
    ydb/library/yql/providers/result/provider
    ydb/library/yql/providers/s3/provider
)

YQL_LAST_ABI_VERSION()

END()
