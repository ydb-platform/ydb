PY3_LIBRARY()

PY_SRCS(
    __init__.py
    actors_core.py
    cli.py
    common.py
    config.py
    runner.py
    results.py
    system_info.py
    topology.py
)

PEERDIR(
    contrib/python/PyYAML
    ydb/tools/ydb_bench/benchmarks
)

END()
