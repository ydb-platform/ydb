PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

PEERDIR(
    library/cpp/lwtrace/protos
)

SRCS(
    certificate.proto
    diagnostics.proto
    endpoints.proto
    media.proto
    rdma.proto
    request_source.proto
    throttler.proto
    trace.proto
)

END()
