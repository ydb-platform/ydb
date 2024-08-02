PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PY_SRCS(
    clickhouse.py
    postgresql.py
    ydb.py
)

PEERDIR(
    contrib/python/clickhouse-connect
    contrib/python/pg8000
    ydb/public/sdk/python
    ydb/library/yql/providers/generic/connector/tests/utils
)

END()
