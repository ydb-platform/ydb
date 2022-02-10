PROTO_LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    viewer.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO) 
 
END()
