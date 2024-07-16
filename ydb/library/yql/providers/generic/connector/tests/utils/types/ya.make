PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PY_SRCS(
    clickhouse.py
    mysql.py
    postgresql.py
    ydb.py
)

END()
