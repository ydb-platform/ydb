PROTO_LIBRARY()

SRCS(
    source.proto
    range.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()

