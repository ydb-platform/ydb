PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    ydb/tests/stress/common
    library/python/monlib
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)


END()
