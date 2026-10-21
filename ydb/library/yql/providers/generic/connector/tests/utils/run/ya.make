PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    kqprun.py
    parent.py
    result.py
    runners.py
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/PyYAML
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/public/api/protos
)

END()
