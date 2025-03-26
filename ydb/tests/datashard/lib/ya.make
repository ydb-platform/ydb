PY3_LIBRARY()

PY_SRCS(
    create_table.py
)

PEERDIR(
    ydb/tests/stress/oltp_workload/workload
)

END()