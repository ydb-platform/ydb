PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

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
