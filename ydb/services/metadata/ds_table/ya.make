LIBRARY()

SRCS(
    accessor_refresh.cpp
    accessor_snapshot_base.cpp
    accessor_snapshot_simple.cpp
    accessor_subscribe.cpp
    behaviour_registrator_actor.cpp
    config.cpp
    registration.cpp
    scheme_describe.cpp
    scheme_transaction.cpp
    service.cpp
    table_exists.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/local_rpc
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/library/actors/core
    ydb/services/metadata/common
    ydb/services/metadata/initializer
    ydb/services/metadata/secret
)

END()

RECURSE_FOR_TESTS(
    ut
)
