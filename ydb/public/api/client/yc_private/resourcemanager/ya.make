PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_private/oauth
    ydb/public/api/client/yc_private/operation
    ydb/public/api/client/yc_private/servicecontrol
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    cloud_service.proto
    folder.proto
    folder_service.proto
    transitional/folder_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

