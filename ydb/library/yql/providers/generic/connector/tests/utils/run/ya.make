PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()


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
