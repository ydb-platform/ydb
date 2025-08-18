PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

GRPC()

SRCS(
    connector.proto
)

# Go is excluded because it's excluded in YDB protofiles
ONLY_TAGS(
    CPP_PROTO
    PY_PROTO
    PY3_PROTO
    JAVA_PROTO
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/service/protos
)

END()
