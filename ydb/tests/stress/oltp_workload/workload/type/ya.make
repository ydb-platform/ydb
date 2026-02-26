PY3_LIBRARY()

PY_SRCS(
    fulltext_index.py
    vector_index.py
    insert_delete_all_types.py
    select_partition.py
    secondary_index.py
    tli.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/library/fixtures
    ydb/public/sdk/python
    ydb/tests/datashard/lib
)

END()
