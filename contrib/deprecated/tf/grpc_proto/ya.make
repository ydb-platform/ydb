PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(1.10.1)

LICENSE(Apache-2.0)

EXCLUDE_TAGS(
    GO_PROTO
    JAVA_PROTO
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/deprecated/tf
)

PY_NAMESPACE(.)

GRPC()

PEERDIR(
    contrib/deprecated/tf/proto
)

SRCDIR(contrib/deprecated/tf)

SRCS(
    tensorflow/core/debug/debug_service.proto
)

END()
