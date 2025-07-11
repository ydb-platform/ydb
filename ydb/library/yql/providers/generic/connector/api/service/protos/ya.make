PROTO_LIBRARY()

PEERDIR(
    yql/essentials/providers/common/proto
    ydb/public/api/protos
)

# Because Go is excluded in YDB protofiles
EXCLUDE_TAGS(GO_PROTO)

SRCS(
    connector.proto
    error.proto
)

END()
