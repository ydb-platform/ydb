PROTO_LIBRARY()

OWNER(g:yql)

SRCS(
    common.proto
    yql_mount.proto 
    clickhouse.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
