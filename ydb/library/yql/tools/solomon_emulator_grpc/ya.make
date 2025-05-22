PY3_PROGRAM(solomon_recipe_grpc)

PY_SRCS(__main__.py)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common

    contrib/python/grpcio
    ydb/library/yql/providers/solomon/solomon_accessor/grpc
)

END()
