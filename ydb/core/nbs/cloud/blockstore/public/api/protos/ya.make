PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    headers.proto
    io.proto
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/lwtrace/protos
)

CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
