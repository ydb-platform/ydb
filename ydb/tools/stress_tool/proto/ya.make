PROTO_LIBRARY()

GRPC()


PEERDIR(
    ydb/core/protos
)

SRCS(
    device_perf_test.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
