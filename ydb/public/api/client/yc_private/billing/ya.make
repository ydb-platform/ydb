PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    billable_object.proto
    billing_account.proto
    billing_account_service.proto
    budget.proto
    budget_service.proto
    customer.proto
    customer_service.proto
    light_metric.proto
    operation_service.proto
    service.proto
    service_service.proto
    sku.proto
    sku_service.proto
    usage.proto
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
    contrib/ydb/public/api/client/yc_private/common
    contrib/ydb/public/api/client/yc_private/access
    contrib/ydb/public/api/client/yc_private/operation
)
END()

