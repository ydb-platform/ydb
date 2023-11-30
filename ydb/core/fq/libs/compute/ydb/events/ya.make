LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/protos
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/ydb_query
)

END()
