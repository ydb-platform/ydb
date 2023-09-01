LIBRARY()

SRCS(
    kqp_federated_query_helpers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/grpc
    ydb/library/db_pool/protos
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()
