PROTO_LIBRARY()

PROTOC_FATAL_WARNINGS()

SRCS(
    ../hnsw.proto
)

PEERDIR(
    ydb/public/api/protos
)

END()
