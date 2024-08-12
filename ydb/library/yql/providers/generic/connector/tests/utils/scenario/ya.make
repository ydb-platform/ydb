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

PEERDIR(
    contrib/python/clickhouse-connect
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/tests/utils/clients
    ydb/library/yql/providers/generic/connector/tests/utils/run
)

END()
