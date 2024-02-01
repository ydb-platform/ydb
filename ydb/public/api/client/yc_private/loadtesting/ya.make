PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    agent_version_service.proto
    moverload_service.proto
    operation_service.proto
    regression_chart_job_annotation.proto
    regression_dashboard_service.proto
    regression_service.proto
    resource_preset.proto
    resource_preset_service.proto
    storage.proto
    storage_service.proto
    tank_instance.proto
    tank_instance_service.proto
    tank_job.proto
    tank_job_service.proto
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
    contrib/ydb/public/api/client/yc_private/billing/v1
    contrib/ydb/public/api/client/yc_private/compute/v1
    contrib/ydb/public/api/client/yc_private/dynamicform/v1
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/common
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/comparison
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/job
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/job/monitoring
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/job/tags
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/moverload
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/regression
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/report
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/report/chart
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/report/table
    contrib/ydb/public/api/client/yc_private/operation
)
END()

