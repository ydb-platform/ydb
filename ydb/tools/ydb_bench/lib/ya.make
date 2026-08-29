PY3_LIBRARY()

PY_SRCS(
    __init__.py
    actors_core.py
    cli.py
    common.py
    config.py
    import_results.py
    load_control.py
    local_ydb.py
    local_ydb_workloads.py
    linux_telemetry.py
    runner.py
    results.py
    system_info.py
    topology.py
    web.py
)

PEERDIR(
    contrib/python/grpcio
    contrib/python/PyYAML
    ydb/core/protos
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tools/ydb_bench/benchmarks
)

END()
