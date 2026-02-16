PY3_LIBRARY()

PY_SRCS(
    __init__.py
    kikimr_keyvalue_client.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/kv_volume/protos
    ydb/public/sdk/python
    library/python/resource
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
