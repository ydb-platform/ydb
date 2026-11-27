YQL_LIBRARY()

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
    library/cpp/digest/md5
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/library/accessor
    ydb/services/metadata/abstract
    ydb/services/metadata/request
    ydb/services/metadata/secret/accessor
)

END()

RECURSE_FOR_TESTS(
    ut
)