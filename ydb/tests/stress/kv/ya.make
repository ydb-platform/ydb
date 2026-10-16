PY3_PROGRAM(workload_kv)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/kv/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)