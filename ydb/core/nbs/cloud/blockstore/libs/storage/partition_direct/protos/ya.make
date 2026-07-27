PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

SRCS(
    partition_direct.proto
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/config/protos
    ydb/core/protos
)

END()
