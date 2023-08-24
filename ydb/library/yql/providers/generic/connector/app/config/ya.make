PROTO_LIBRARY()

ONLY_TAGS(GO_PROTO)

SRCS(
    client.proto
    server.proto
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/common
)

END()
