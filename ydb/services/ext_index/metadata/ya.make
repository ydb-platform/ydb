LIBRARY()

SRCS(
    GLOBAL object.cpp
    GLOBAL behaviour.cpp
    manager.cpp
    initializer.cpp
    snapshot.cpp
    fetcher.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
    ydb/services/metadata/request
    ydb/core/tx/sharding
)

YQL_LAST_ABI_VERSION()

END()
