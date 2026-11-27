YQL_LIBRARY()

SRCS(
    accessor_init.cpp
    behaviour.cpp
    common.cpp
    events.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
    initializer.cpp
    fetcher.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/time_provider
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/library/accessor
    ydb/library/actors/core
    ydb/services/metadata/abstract
    ydb/services/metadata/request
)

END()

RECURSE_FOR_TESTS(
    ut
)