PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/protos
)

ONLY_TAGS(CPP_PROTO PY3_PROTO)

SRCS(
    connector.proto
)

END()
