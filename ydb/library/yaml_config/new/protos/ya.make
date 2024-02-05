PROTO_LIBRARY(parser-protos)

SRCS(
    message.proto
)

PEERDIR(
    ydb/core/protos
)

CPP_PROTO_PLUGIN0(validation ydb/core/driver_lib/validation)

EXCLUDE_TAGS(GO_PROTO)

END()
