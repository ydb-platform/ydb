LIBRARY()

SRCS(
    object.cpp
    GLOBAL behaviour.cpp
    manager.cpp
    initializer.cpp
    snapshot.cpp
    fetcher.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
    ydb/services/metadata/request
    ydb/services/ext_index/metadata/extractor
)

END()
