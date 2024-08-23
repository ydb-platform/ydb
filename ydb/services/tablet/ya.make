LIBRARY()

SRCS(
    ydb_tablet.cpp
)

PEERDIR(
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/tablet
)

END()
