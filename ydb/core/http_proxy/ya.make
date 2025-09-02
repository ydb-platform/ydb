LIBRARY()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    auth_factory.cpp
    auth_factory.h
    custom_metrics.h
    discovery_actor.cpp
    discovery_actor.h
    events.h
    exceptions_mapping.cpp
    exceptions_mapping.h
    grpc_service.cpp
    grpc_service.h
    http_req.cpp
    http_req.h
    http_service.cpp
    http_service.h
    json_proto_conversion.h
    metrics_actor.cpp
    metrics_actor.h
)

PEERDIR(
    contrib/libs/grpc
    contrib/restricted/nlohmann_json
    ydb/library/actors/http
    ydb/library/actors/core
    ydb/library/grpc/actor_client
    ydb/core/base
    ydb/core/protos
    ydb/core/grpc_services/local_rpc
    ydb/core/security
    yql/essentials/public/issue
    ydb/library/http_proxy/authorization
    ydb/library/http_proxy/error
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/library/naming_conventions
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic/codecs
    ydb/public/sdk/cpp/src/client/iam_private
    ydb/services/datastreams
    ydb/services/persqueue_v1/actors
    ydb/services/ymq
    ydb/public/api/grpc
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
