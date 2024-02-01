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
    ydb/public/api/client/yc_public/api
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/compute
    ydb/public/api/client/yc_private/loadtesting/common
    ydb/public/api/client/yc_private/loadtesting/comparison
    ydb/public/api/client/yc_private/loadtesting/job
    ydb/public/api/client/yc_private/loadtesting/job/monitoring
    ydb/public/api/client/yc_private/loadtesting/job/tags
    ydb/public/api/client/yc_private/loadtesting/moverload
    ydb/public/api/client/yc_private/loadtesting/regression
    ydb/public/api/client/yc_private/loadtesting/report
    ydb/public/api/client/yc_private/loadtesting/report/chart
    ydb/public/api/client/yc_private/loadtesting/report/table
    ydb/public/api/client/yc_private/operation
)
END()

