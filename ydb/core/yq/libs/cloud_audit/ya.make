LIBRARY()

SRCS(
    yq_cloud_audit_service.cpp
)

PEERDIR(
    library/cpp/actors/log_backend
    library/cpp/unified_agent_client
    ydb/core/yq/libs/actors
    ydb/core/yq/libs/audit/events
    ydb/core/yq/libs/config/protos
    ydb/library/folder_service
    ydb/library/ycloud/api
    ydb/public/api/client/yc_public/events
)

END()
