PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    nbs2_load.proto
)

PEERDIR(
    library/cpp/lwtrace/protos
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()

