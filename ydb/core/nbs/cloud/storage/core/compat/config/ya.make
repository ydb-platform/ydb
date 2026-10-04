PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    grpc_client.proto
    iam.proto
    opentelemetry_client.proto
)

END()
