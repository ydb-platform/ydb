PY3_LIBRARY()

PY_SRCS(
    base.py
)

PEERDIR(
    ydb/tests/functional/tpc/lib
    ydb/tests/workload_manager/common
)

END()
