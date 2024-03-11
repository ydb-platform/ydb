PROTO_LIBRARY()

SRCS(
    events.proto
    blobs.proto
)

PEERDIR(
    ydb/library/actors/protos
)

END()
