PY3_LIBRARY()

PY_SRCS(
    vector_index.py
    insert_delete_all_types.py
<<<<<<< HEAD
    select_partition.py
=======
    vector_index_large_levels_and_clusters.py
>>>>>>> 72b75df7322 (add test)
)

PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/tests/datashard/lib
)

END()
