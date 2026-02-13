PY3_LIBRARY()

PY_SRCS(
    __init__.py
    fulltext.py
    safe_parametrize.py
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
)

END()
