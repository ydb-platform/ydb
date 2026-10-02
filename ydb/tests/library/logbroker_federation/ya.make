PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/grpcio
    contrib/python/ydb/py3
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tests/library
)

END()
