SUBSCRIBER(g:kikimr)

PY3_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    catalog.py
    disk.py
    node.py
    tablet.py
    monitor.py
)

PEERDIR(
    contrib/python/Flask
    ydb/tests/library
    library/python/monlib
    ydb/core/protos
)

END()
