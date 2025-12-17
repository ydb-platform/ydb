PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/restricted/grpc_py2
)

GRPC()

EXCLUDE_TAGS(GO_PROTO)

END()
