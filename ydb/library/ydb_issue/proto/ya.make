PROTO_LIBRARY()

GRPC()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    issue_id.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
