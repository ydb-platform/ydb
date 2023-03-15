PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_public/common
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    log_entry.proto
    log_ingestion_service.proto
    log_resource.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

