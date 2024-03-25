PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    artifacts.py
    comparator.py
    data_source_kind.py
    database.py
    dqrun.py
    docker_compose.py
    generate.py
    kqprun.py
    log.py
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

RECURSE_FOR_TESTS(
    types
)
