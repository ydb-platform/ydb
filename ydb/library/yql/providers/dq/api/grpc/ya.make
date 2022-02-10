PROTO_LIBRARY()

GRPC()

OWNER(
    g:yql
)

SRCS(
    api.proto
)

PEERDIR(
    ydb/library/yql/providers/dq/api/protos
)

EXCLUDE_TAGS(GO_PROTO) 
 
END()
