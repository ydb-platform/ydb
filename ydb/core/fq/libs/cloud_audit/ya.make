LIBRARY()

SRCS(
    yq_cloud_audit_service.cpp
)

PEERDIR(
    library/cpp/unified_agent_client
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/audit/events
    ydb/core/fq/libs/config/protos
    ydb/library/actors/log_backend
    ydb/library/folder_service
    ydb/library/security
    ydb/library/ycloud/api
    ydb/public/api/client/yc_public/events
)

END()
