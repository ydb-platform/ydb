LIBRARY()

SRCS(
    accessor_refresh.cpp
    accessor_subscribe.cpp
    accessor_snapshot_simple.cpp
    accessor_snapshot_base.cpp
    behaviour_registrator_actor.cpp
    scheme_describe.cpp
    table_exists.cpp
    service.cpp
    config.cpp
    registration.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/services/metadata/common
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
    ydb/services/metadata/initializer
    ydb/services/metadata/secret
)

END()
