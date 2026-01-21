PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)

SRCS(
    lwtrace.proto
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
