PROTO_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(
    g:cpp-contrib
    g:quasar-sys
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/libs/nanopb/generator/proto
)

PY_NAMESPACE(.)

SRCS(
    nanopb.proto
)

END()
