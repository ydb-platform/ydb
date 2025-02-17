PROTO_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

GRPC()

SRCS(
    channelz.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
