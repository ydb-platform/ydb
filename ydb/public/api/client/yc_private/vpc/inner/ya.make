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
    contrib/ydb/public/api/client/yc_public/api
    contrib/ydb/public/api/client/yc_public/api/tools
    contrib/ydb/public/api/client/yc_private/common
    contrib/ydb/public/api/client/yc_private/billing/v1
    contrib/ydb/public/api/client/yc_private/compute/v1
    contrib/ydb/public/api/client/yc_private/loadbalancer/v1
    contrib/ydb/public/api/client/yc_private/operation
    contrib/ydb/public/api/client/yc_private/quota
    contrib/ydb/public/api/client/yc_private/servicecontrol/v1
    contrib/ydb/public/api/client/yc_private/vpc/v1
)
END()

