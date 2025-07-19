PY3_LIBRARY()

    PY_SRCS (
        column_table_helper.py
        range_allocator.py
        s3_client.py
        thread_helper.py
        time_histogram.py
        utils.py
        ydb_client.py
    )

    PEERDIR(
        contrib/python/boto3
        contrib/python/numpy
        contrib/python/requests
        library/recipes/common
        ydb/public/sdk/python
    )

END()
