PY3_LIBRARY()

PY_SRCS(
    vector_index.py
    insert_delete_all_types.py
    select_partition.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/tests/datashard/lib
)

END()
