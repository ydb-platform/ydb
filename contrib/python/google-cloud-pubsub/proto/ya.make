LIBRARY()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PROTO_NAMESPACE(contrib/python/google-cloud-pubsub/proto)

GRPC()

PEERDIR(
    contrib/python/grpc-google-iam-v1
)

SRCS(
    google/pubsub/v1/pubsub.proto
    google/pubsub/v1/schema.proto
)

END()
