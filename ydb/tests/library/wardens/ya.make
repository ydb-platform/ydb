PY23_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    datashard.py
    disk.py
    factories.py
    hive.py
    logs.py
    schemeshard.py
)

PEERDIR(
    ydb/tests/library/clients
)

END()