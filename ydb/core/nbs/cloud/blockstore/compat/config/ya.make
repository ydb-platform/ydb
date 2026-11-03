PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    client.proto
    diagnostics.proto
    server.proto
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos
    ydb/core/nbs/cloud/storage/core/compat/config
    ydb/core/nbs/cloud/storage/core/compat/protos
)

END()
