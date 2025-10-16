PY3_LIBRARY()

PY_SRCS(
    test_base.py
    helpers.py
    test_query.py
    test_s3.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    library/python/testing/recipe
    library/recipes/common
    contrib/python/moto
)

END()
