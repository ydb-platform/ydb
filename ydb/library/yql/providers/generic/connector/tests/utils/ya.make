PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    clickhouse.py
    comparator.py
    database.py
    dqrun.py
    generate.py
    kqprun.py
    log.py
    postgresql.py
    runner.py
    schema.py
    settings.py
    sql.py
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/common
    ydb/public/api/protos
    yt/python/yt/yson
)

END()
