PY3_LIBRARY()

PY_SRCS(
    compression.py
    data_types.py
    mixed.py
    schema_inclusion.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/stress/common
    ydb/public/sdk/python
    contrib/python/pyarrow
)

END()
