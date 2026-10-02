PY3_LIBRARY()

    PY_SRCS (
        build_report.py
        deploy.py
        run_stress.py
        workload_executor_parallel.py
    )

    PEERDIR (
        ydb/tests/library/stability/utils
        ydb/tests/library/stability/healthcheck
    )

END()
