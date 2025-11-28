PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    service.proto
    dqs.proto
    task_command_executor.proto
)

PEERDIR(
    ydb/library/actors/protos
    ydb/public/api/protos
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/dq/proto
    yql/essentials/providers/common/metrics/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
