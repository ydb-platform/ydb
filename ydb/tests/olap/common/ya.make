PY3_LIBRARY()

    PY_SRCS (
        column_table_helper.py
        thread_helper.py
        ydb_client.py
    )

    PEERDIR(
        ydb/public/sdk/python
    )

END()
