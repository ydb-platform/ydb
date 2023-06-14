PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/protos
)

# Because Go is excluded in YDB protofiles
EXCLUDE_TAGS(GO_PROTO)

SRCS(
    connector.proto
)

END()
