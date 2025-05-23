PY3_LIBRARY()

PY_SRCS(
    batch_update_all_types.py
    batch_delete_all_types.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/tests/datashard/lib
)

END()
