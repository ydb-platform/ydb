PY3_PROGRAM(iam_grpc_recipe)

PY_SRCS(__main__.py)

PEERDIR(
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
