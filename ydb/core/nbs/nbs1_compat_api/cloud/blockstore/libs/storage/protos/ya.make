PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    volume.proto
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos
)

END()
