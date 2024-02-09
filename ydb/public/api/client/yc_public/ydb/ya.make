PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    backup.proto
    backup_service.proto
    database.proto
    database_service.proto
    location.proto
    location_service.proto
    resource_preset.proto
    resource_preset_service.proto
    storage_type.proto
    storage_type_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/code
    rpc/errdetails
    rpc/status
    type/timeofday
    type/dayofweek
)

PEERDIR(
    ydb/public/api/client/yc_common/api
    ydb/public/api/client/yc_public/common
    ydb/public/api/client/yc_public/access
    ydb/public/api/client/yc_public/operation
)
END()

