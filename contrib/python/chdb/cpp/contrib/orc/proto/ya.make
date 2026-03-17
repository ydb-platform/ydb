PROTO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    LicenseRef-scancode-unknown-license-reference AND
    MIT
)

WITHOUT_LICENSE_TEXTS()

VERSION(3.3.0)

CFLAGS(
    -DCPP_PROTO_PLUGINS=lite:
    -DPROTOC_STYLEGUIDE_OUT=""
    -DPROTOC_PLUGIN_STYLEGUIDE=""
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/python/chdb/cpp/contrib/orc/proto
)

SRCS(
    orc_chdb_proto.proto
)

END()
