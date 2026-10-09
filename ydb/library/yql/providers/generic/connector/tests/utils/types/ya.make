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

END()
