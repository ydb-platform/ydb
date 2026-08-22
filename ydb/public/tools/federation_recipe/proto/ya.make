PROTO_LIBRARY()

GRPC()

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    validation.proto
    ydb_common.proto
    ydb_issue_message.proto
    ydb_status_codes.proto
    ydb_operation.proto
    common.proto
    config_manager_admin.proto
    config_manager.proto
    config_manager_service.proto
    config_manager_admin_service.proto
)

END()
