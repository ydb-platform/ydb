PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    clickhouse.py
    ms_sql_server.py
    mysql.py
    oracle.py
    postgresql.py
    ydb.py
)

PEERDIR(
    contrib/python/clickhouse-connect
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/tests/utils/clients
    ydb/library/yql/providers/generic/connector/tests/utils/run
    ydb/library/yql/providers/generic/connector/tests/common_test_cases
)

END()
