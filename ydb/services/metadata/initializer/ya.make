LIBRARY()

SRCS(
    accessor_init.cpp
    behaviour.cpp
    common.cpp
    controller.cpp
    events.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
    initializer.cpp
    fetcher.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
    ydb/services/metadata/request
)

END()

RECURSE_FOR_TESTS(
    ut
)