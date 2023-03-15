PROTO_LIBRARY()

SRCS(
    service.proto
    dqs.proto
    task_command_executor.proto
)

PEERDIR(
    library/cpp/actors/protos
    ydb/public/api/protos
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/common/metrics/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
