PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/public/api/client/yc_private/operation
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    access_service.proto
    resource.proto
    service_control_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
