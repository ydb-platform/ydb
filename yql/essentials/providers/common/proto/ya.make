PROTO_LIBRARY()

SRCS(
    gateways_config.proto
    udf_resolver.proto
)

PEERDIR(
    yql/essentials/protos
    yql/essentials/utils/fetch/proto
    yql/essentials/minikql/runtime_settings/proto
    yql/essentials/providers/common/proto/activation
)

EXCLUDE_TAGS(GO_PROTO)

END()
