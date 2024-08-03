PY3_LIBRARY()

# YQ-3351: enabling python style checks only for opensource
NO_LINT()

PY_SRCS(
    clickhouse.py
    mysql.py
    postgresql.py
    ydb.py
)

END()
