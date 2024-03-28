PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    clickhouse.py
    postgresql.py
)

PEERDIR(
    contrib/python/clickhouse-connect
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
)

END()
