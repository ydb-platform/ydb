PROTO_LIBRARY()

GRPC()

SRCS(
    connector.proto
)

# Go is excluded because it's excluded in YDB protofiles
ONLY_TAGS(
    CPP_PROTO
    PY_PROTO
    PY3_PROTO
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/service/protos
)

END()
