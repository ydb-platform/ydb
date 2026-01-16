PY3_LIBRARY()

PY_SRCS(
    __init__.py
    fulltext.py
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
)

END()
