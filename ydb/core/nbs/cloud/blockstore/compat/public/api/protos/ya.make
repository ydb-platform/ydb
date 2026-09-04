PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    encryption.proto
    headers.proto
    io.proto
    mount.proto
    ping.proto
    rdma.proto
    volume_throttling.proto
    volume.proto
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/compat/protos
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/lwtrace/protos
)

END()
