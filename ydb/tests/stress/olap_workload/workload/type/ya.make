PY3_LIBRARY()

PY_SRCS(
    tables_create_drop.py
    insert_delete.py
)

PEERDIR(
    ydb/tests/stress/common
)

END()
