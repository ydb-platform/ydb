PROTO_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

WITHOUT_LICENSE_TEXTS()

SUBSCRIBER(
    g:cpp-contrib
    g:quasar-sys
)

SRCS(
    nanopb.proto
)

END()
