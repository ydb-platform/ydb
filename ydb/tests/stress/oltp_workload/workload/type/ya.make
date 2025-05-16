PY3_LIBRARY()

PY_SRCS(
    vector_index.py
    insert_delete_all_types.py
    vector_index_large_levels_and_clusters.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/tests/datashard/lib
)

END()
