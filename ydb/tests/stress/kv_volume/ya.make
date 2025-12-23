PY3_PROGRAM(workload_keyvalue_volume)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/library/clients
    ydb/tests/library
    ydb/tests/stress/kv_volume/workload
)

END()
