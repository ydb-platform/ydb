PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/protos/annotations
)

SRCS(
    validation_test.proto
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

EXCLUDE_TAGS(GO_PROTO)

END()
