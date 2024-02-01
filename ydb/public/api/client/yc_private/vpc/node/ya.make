PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    port_controller_service.proto
    port_device.proto
    port_device_service.proto
    vrouter_slot.proto
    vrouter_slot_service.proto
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
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/vpc/v1/inner
)
END()

