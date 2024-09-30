PY3_LIBRARY()

PY_SRCS(
    artifacts.py
    comparator.py
    data_source_kind.py
    database.py
    docker_compose.py
    generate.py
    log.py
    schema.py
    settings.py
    sql.py
)

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PEERDIR(
    contrib/python/PyYAML
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/tests/utils/types
    ydb/public/api/protos
    yt/python/yt/yson
)

END()

RECURSE_FOR_TESTS(
    clients
    run
    scenario
    types
)
