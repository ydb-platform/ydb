PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

SRCS(
    nbs2_load.proto
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
