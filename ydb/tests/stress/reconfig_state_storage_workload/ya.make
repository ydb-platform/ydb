PY3_PROGRAM(reconfig_state_storage_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/PyHamcrest/py3
    ydb/tests/stress/common
    ydb/tests/stress/reconfig_state_storage_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
