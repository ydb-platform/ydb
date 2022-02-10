PROTO_LIBRARY()

OWNER(
    ilnaz
    g:kikimr
)

PEERDIR(
    ydb/public/api/protos/annotations
)

SRCS(
    validation_test.proto
)

CPP_PROTO_PLUGIN0(validation ydb/core/grpc_services/validation)

EXCLUDE_TAGS(GO_PROTO) 
 
END()
