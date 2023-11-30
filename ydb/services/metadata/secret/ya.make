LIBRARY()

SRCS(
    secret.cpp
    GLOBAL secret_behaviour.cpp
    checker_secret.cpp
    access.cpp
    GLOBAL access_behaviour.cpp
    checker_access.cpp

    manager.cpp
    snapshot.cpp
    initializer.cpp
    fetcher.cpp
)

PEERDIR(
    ydb/library/actors/core
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