PROTO_LIBRARY()

PEERDIR(
    ydb/core/config/proto_options
)

SRCS(
    validation_test.proto
)

CPP_PROTO_PLUGIN0(validation ydb/core/driver_lib/validation)

EXCLUDE_TAGS(GO_PROTO)

END()
