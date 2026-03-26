PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    error.proto
    media.proto
    request_source.proto
    trace.proto
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
