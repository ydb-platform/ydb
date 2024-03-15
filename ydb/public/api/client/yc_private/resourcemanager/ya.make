PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_private/operation
    ydb/public/api/client/yc_private/servicecontrol
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    folder.proto
    transitional/folder_service.proto
    folder_service.proto
    cloud_service.proto
    cloud.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

