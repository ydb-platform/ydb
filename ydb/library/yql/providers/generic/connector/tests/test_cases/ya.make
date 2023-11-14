PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    base.py
    collection.py
    join.py
    select_datetime.py
    select_missing_database.py
    select_missing_table.py
    select_pg_schema.py
    select_positive.py
    select_pushdown.py
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
)

END()
