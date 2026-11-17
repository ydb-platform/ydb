PY3_LIBRARY()

PY_SRCS(
    validate_indexes.py
)

PEERDIR(
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()

RECURSE(
    bin
)
