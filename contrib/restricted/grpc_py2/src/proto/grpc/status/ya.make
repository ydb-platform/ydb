PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/restricted/grpc_py2
)

GRPC()

SRCS(
    status.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
