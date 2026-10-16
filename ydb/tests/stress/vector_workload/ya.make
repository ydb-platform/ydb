PY3_PROGRAM(workload_vector)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/vector_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
