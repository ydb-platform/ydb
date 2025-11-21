PROTO_LIBRARY()

SRCS(
    gateways_config.proto
    udf_resolver.proto
)

PEERDIR(
    yql/essentials/protos
    yql/essentials/utils/fetch/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
