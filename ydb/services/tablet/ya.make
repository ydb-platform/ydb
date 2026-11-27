LIBRARY()

SRCS(
    ydb_tablet.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/tablet
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
)

END()
