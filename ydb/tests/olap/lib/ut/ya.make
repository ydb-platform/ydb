PY3TEST()

SIZE(SMALL)
REQUIREMENTS(cpu:2)

PEERDIR(
    ydb/tests/olap/lib
)

TEST_SRCS(
    test_workload_run_result.py
    test_errors_report.py
)

END()
