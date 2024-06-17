PY3_LIBRARY()

IF (OPENSOURCE) {
    STYLE_PYTHON()
}

PY_SRCS(
    clickhouse.py
    postgresql.py
    ydb.py
)

END()
