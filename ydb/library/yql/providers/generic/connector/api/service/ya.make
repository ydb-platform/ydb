PROTO_LIBRARY()

GRPC()

SRCS(
    connector.proto
)

# Because Go is excluded in YDB protofiles
EXCLUDE_TAGS(GO_PROTO)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/service/protos
)

END()
