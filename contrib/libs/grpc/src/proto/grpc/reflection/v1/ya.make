PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

PY_NAMESPACE(src.proto.grpc.reflection.v1)

GRPC()

SRCS(
    reflection.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
