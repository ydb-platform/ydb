PY23_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    datashard.py
    disk.py
    factories.py
    fetched_counters.py
    hive.py
    logs.py
    pq.py
    schemeshard.py
)

PEERDIR(
    ydb/tests/library/clients
)

END()