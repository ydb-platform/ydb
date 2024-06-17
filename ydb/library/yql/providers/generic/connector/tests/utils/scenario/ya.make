PY3_LIBRARY()

IF (OPENSOURCE) {
    STYLE_PYTHON()
}

PY_SRCS(
    clickhouse.py
    postgresql.py
    ydb.py
)

PEERDIR(
    contrib/python/clickhouse-connect
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/tests/utils/clients
    ydb/library/yql/providers/generic/connector/tests/utils/run
)

END()
