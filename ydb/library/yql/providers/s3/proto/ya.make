PROTO_LIBRARY()

SRCS(
    credentials.proto
    file_queue.proto
    range.proto
    retry_config.proto
    sink.proto
    source.proto
)

PEERDIR(
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()


