OWNER(g:yq)

LIBRARY()

SRCS(
    yq_mock_audit_service.cpp
)

PEERDIR(
    ydb/core/yq/libs/audit/events
    ydb/core/yq/libs/config/protos
)

END()
