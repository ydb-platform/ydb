PROTO_LIBRARY()

GRPC()

SRCS(mock.proto)

PEERDIR(ydb/udfs/wasm/profile/proto/schema)

END()
