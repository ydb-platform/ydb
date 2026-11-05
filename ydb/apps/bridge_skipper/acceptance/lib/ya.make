PY3_LIBRARY()

PY_SRCS(
    __init__.py
    harness.py
)

PEERDIR(
    library/python/port_manager
    ydb/apps/bridge_skipper/ydbd_mock/lib
)

END()
