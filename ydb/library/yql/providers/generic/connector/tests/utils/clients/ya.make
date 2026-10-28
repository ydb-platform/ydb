PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    postgresql.py
)

PEERDIR(
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
)

END()
