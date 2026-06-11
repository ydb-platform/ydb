PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests
    library/python/port_manager
    ydb/library/yql/tools/solomon_emulator/client
    ydb/library/yql/tools/solomon_emulator/lib
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/stress/common
)

END()
