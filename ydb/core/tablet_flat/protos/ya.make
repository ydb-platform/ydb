LIBRARY()

SRCS(
    flat_table_part.proto
    flat_table_shard.proto
)

PEERDIR(
    contrib/libs/protobuf
    ydb/core/protos
    ydb/core/scheme/protos
)

END()
