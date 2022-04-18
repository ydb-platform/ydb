OWNER(g:kikimr)

LIBRARY()


SRCS(
    driver_cache_actor.cpp
    driver_cache_actor.h
    events.h
    http_req.cpp
    http_req.h
    metrics_actor.cpp
    metrics_actor.h
    grpc_service.h
    grpc_service.cpp
    discovery_actor.h
    discovery_actor.cpp
    http_service.h
    http_service.cpp
    auth_factory.h
    auth_factory.cpp
)

PEERDIR(
    library/cpp/actors/http
    library/cpp/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/library/http_proxy/authorization
    ydb/library/http_proxy/error
    ydb/library/naming_conventions
    ydb/public/sdk/cpp/client/ydb_datastreams
    ydb/public/sdk/cpp/client/ydb_persqueue_core
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

YQL_LAST_ABI_VERSION()

END()

