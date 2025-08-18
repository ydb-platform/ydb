SUBSCRIBER(g:kikimr)

PY3_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    bridge_pile.py
    catalog.py
    datacenter.py
    disk.py
    node.py
    tablet.py
    monitor.py
)

PEERDIR(
    contrib/python/Flask
    ydb/tests/library
    ydb/tests/library/clients
    library/python/monlib
    ydb/core/protos
)

END()
