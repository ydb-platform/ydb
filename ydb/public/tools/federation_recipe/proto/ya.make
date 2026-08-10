PROTO_LIBRARY()

GRPC()

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    logbroker/public/api/common/validation.proto
    logbroker/public/api/common/ydb_common.proto
    logbroker/public/api/common/ydb_issue_message.proto
    logbroker/public/api/common/ydb_status_codes.proto
    logbroker/public/api/common/ydb_operation.proto
    logbroker/public/api/common/common.proto
    logbroker/public/api/admin/config_manager_admin.proto
    logbroker/public/api/grpc/config_manager.proto
    logbroker/public/api/grpc/config_manager_admin.proto
    logbroker/public/api/protos/config_manager.proto
)

END()
