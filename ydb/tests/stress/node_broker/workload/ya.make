PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
