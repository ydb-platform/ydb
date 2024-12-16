PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

PY_NAMESPACE("src.proto.grpc.health.v1")

GRPC()

SRCS(
    health.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
