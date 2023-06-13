PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/grpc
)

GRPC()

SRCS(
    status.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
