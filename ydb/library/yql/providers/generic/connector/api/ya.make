PROTO_LIBRARY()

GRPC()

SRCS(
    connector.proto
)

ONLY_TAGS(CPP_PROTO PY3_PROTO)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/protos
)

END()
