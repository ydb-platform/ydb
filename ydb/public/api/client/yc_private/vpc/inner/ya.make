PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    accounting.proto
    accounting_service.proto
    address.proto
    address_service.proto
    backoffice_service.proto
    cms_service.proto
    common.proto
    compute_internal_service.proto
    console_vpc_service.proto
    discovery.proto
    discovery_service.proto
    feature_flag.proto
    feature_flag_service.proto
    fip_bucket.proto
    fip_bucket_service.proto
    internal_operation_service.proto
    network_dns_service.proto
    network_interface_attachment_service.proto
    network_service.proto
    node_registry.proto
    node_registry_service.proto
    port_state.proto
    port_state_service.proto
    semaphore.proto
    semaphore_service.proto
    statistic_service.proto
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
    ydb/public/api/client/yc_common/api/tools
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/billing
    ydb/public/api/client/yc_private/compute
    ydb/public/api/client/yc_private/loadbalancer
    ydb/public/api/client/yc_private/operation
    ydb/public/api/client/yc_private/quota
    ydb/public/api/client/yc_private/servicecontrol
    ydb/public/api/client/yc_private/vpc
)
END()

