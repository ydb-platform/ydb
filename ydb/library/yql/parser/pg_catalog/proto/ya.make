PROTO_LIBRARY()

SRCS(
    pg_catalog.proto
)

PEERDIR(
    ydb/library/yql/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
