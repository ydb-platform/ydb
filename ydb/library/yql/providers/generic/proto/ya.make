PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/service/protos
    yql/essentials/providers/common/proto
)

SRCS(
    partition.proto
    source.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()
