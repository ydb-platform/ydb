PY3TEST()

SIZE(SMALL)

PEERDIR(
    ydb/tests/olap/lib
)

TEST_SRCS(
    test_workload_run_result.py
    test_errors_report.py
)

END()
