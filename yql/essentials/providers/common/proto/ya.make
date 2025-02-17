PROTO_LIBRARY()

SRCS(
    gateways_config.proto
    udf_resolver.proto
)

PEERDIR(
    yql/essentials/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
