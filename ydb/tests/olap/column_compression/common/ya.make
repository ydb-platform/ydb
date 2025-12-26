PY3_LIBRARY()

    PY_SRCS (
        base.py
    )

    PEERDIR(
        ydb/tests/library
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/scenario/helpers
        ydb/tests/olap/common
    )

END()
