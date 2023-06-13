PROTO_LIBRARY()

SRCS(
    range.proto
    source.proto
)

IF (NOT PY_PROTOS_FOR)
    EXCLUDE_TAGS(GO_PROTO)
ENDIF()

END()

