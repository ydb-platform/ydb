OWNER(g:yq)
 
PROTO_LIBRARY() 
 
PEERDIR( 
    ydb/public/api/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/dq/api/protos
) 
 
SRCS( 
    yq_internal.proto 
) 
 
EXCLUDE_TAGS(GO_PROTO) 
 
END() 
