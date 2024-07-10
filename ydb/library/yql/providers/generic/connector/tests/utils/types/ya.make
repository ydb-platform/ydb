PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

IF (OPENSOURCE) 
    # YQ-3351: enabling python style checks only for opensource
    STYLE_PYTHON()
ENDIF()

PY_SRCS(
    clickhouse.py
    postgresql.py
    ydb.py
)

END()
