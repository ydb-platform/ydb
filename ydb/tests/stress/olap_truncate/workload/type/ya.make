PY3_LIBRARY()

PY_SRCS(
    truncate_insert.py
    truncate_concurrent.py
)

PEERDIR(
    ydb/tests/stress/common
)

END()
