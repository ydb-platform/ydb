PY3_PROGRAM(s3_recipe)

PY_SRCS(__main__.py)

PEERDIR(
    contrib/python/requests
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
)

FILES(
    start.sh
    stop.sh
)

END()
