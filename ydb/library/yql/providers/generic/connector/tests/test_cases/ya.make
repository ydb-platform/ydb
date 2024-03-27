PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    base.py
    select_missing_database.py
    select_missing_table.py
    select_positive_common.py
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
)

END()
