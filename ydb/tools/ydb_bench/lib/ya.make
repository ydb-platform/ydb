PY3_LIBRARY()

PY_SRCS(
    __init__.py
    actors_core.py
    cli.py
    common.py
    config.py
    import_results.py
    runner.py
    results.py
    system_info.py
    topology.py
    web.py
)

PEERDIR(
    contrib/python/PyYAML
    ydb/tools/ydb_bench/benchmarks
)

END()
