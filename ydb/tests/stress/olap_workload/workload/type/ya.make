PY3_LIBRARY()

PY_SRCS(
    tables_create_drop.py
    insert_delete.py
    transactions.py
    rename_tables.py
    encodings.py
    move_data.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/common
)

END()
