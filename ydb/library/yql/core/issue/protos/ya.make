PROTO_LIBRARY()

OWNER(g:yql g:yql_ydb_core)

SRCS(
    issue_id.proto
)

PEERDIR(
    ydb/library/yql/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO) 
 
END()
