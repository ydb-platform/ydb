LIBRARY()

SRCS(
    http_req.cpp
    http_req.h
    http_router.cpp
    http_router.h
    http_service.cpp
    http_service.h
    grpc_request_context_wrapper.cpp
    grpc_request_context_wrapper.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/http
    library/cpp/protobuf/json
    library/cpp/resource
    ydb/core/base
    ydb/core/http_proxy
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/core/public_http/protos
    ydb/core/viewer/json
    ydb/core/yq/libs/result_formatter
    ydb/library/yql/public/issue
    ydb/public/sdk/cpp/client/ydb_types

)

RESOURCE(
    openapi/openapi.yaml resources/openapi.yaml
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
