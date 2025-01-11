PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

PY_SRCS(
    test_base.py
    test_lib.py
    test_query.py
    test_s3.py
)

PEERDIR(
    ydb/tests/library
    library/python/testing/recipe
    library/recipes/common
)

END()
