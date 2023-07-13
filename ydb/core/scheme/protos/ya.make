PROTO_LIBRARY()

GRPC()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    type_info.proto
    key_range.proto
    pathid.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
