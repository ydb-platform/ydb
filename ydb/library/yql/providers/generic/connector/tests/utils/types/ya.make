PY3_LIBRARY()

IF (OPENSOURCE) 
    STYLE_PYTHON()
ENDIF()

PY_SRCS(
    clickhouse.py
    postgresql.py
    ydb.py
)

END()
