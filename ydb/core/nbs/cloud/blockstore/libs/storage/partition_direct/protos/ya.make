PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

SRCS(
    partition_direct.proto
)

PEERDIR(
    ydb/core/protos
)

#CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
