PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/stress/common
    ydb/tests/datashard/lib
    ydb/tests/stress/olap_workload/workload/type
)

END()
