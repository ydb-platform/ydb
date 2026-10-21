PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

SRCS(
    dirty_map.proto
    partition_direct.proto
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/config/protos
    ydb/core/protos
)

END()
