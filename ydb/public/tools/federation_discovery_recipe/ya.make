PY3_PROGRAM(federation_discovery_recipe)

PY_SRCS(__main__.py)

PEERDIR(
    contrib/python/grpcio
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/public/api/grpc
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    bin
)
