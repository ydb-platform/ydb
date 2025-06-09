PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

PY_SRCS(
    dml_operations.py
    create_table.py
    types_of_variables.py
    multicluster_test_base.py
    vector_base.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

END()
