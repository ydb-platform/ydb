PROTO_LIBRARY()

SRCS(
    common.proto
    yql_mount.proto
    clickhouse.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
