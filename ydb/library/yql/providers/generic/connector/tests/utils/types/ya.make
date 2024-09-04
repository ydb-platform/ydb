PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PY_SRCS(
    clickhouse.py
    ms_sql_server.py
    mysql.py
    oracle.py
    postgresql.py
    ydb.py
)

END()
