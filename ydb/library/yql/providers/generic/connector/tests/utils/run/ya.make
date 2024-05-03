PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    dqrun.py
    kqprun.py
    parent.py
    result.py
    runners.py
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/PyYAML
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/public/api/protos
    yt/python/yt/yson
)

END()
