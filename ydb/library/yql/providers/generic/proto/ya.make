PROTO_LIBRARY()

ONLY_TAGS(CPP_PROTO)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/protos
)

SRCS(
    range.proto
    source.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()

