PY3_LIBRARY()

PY_SRCS(
    __init__.py
    cluster.py
    config_yaml.py
    connectivity.py
    control_api.py
    events.py
    faults.py
    followups.py
    health.py
    instance.py
    lifecycle.py
    model.py
    operations.py
    peers.py
    replica.py
    services.py
    supervisor.py
    testkit.py
    topology.py
    validator.py
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    contrib/python/grpcio
    contrib/python/protobuf
    contrib/python/PyYAML
    contrib/python/aiohttp
)

END()
