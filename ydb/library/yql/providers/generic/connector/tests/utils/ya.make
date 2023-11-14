PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    clickhouse.py
    database.py
    dqrun.py
    log.py
    postgresql.py
    schema.py
    settings.py
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/common
    ydb/public/api/protos
    yt/python/yt/yson
)

END()
