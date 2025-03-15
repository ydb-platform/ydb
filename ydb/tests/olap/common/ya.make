PY3_LIBRARY()

    PY_SRCS (
        column_table_helper.py
        s3_client.py
        thread_helper.py
        ydb_client.py
    )

    PEERDIR(
        contrib/python/boto3
        library/recipes/common
        ydb/public/sdk/python
    )

END()
