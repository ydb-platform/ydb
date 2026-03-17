PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(3.5.6)

LICENSE(Apache-2.0)

PROTO_NAMESPACE(contrib/python/pyspark)

PY_NAMESPACE(pyspark.sql.connect.proto)

GRPC()

SRCS(
    base.proto
    catalog.proto
    commands.proto
    common.proto
    example_plugins.proto
    expressions.proto
    relations.proto
    types.proto
)

END()
