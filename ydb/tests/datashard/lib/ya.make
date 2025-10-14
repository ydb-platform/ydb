PY3_LIBRARY()

PY_SRCS(
    dml_operations.py
    create_table.py
    types_of_variables.py
    multicluster_test_base.py
    vector_base.py
    vector_index.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

END()
