PY3_LIBRARY()

PY_SRCS(
    __init__.py
    actors_core.py
    cli.py
    common.py
    config.py
    runner.py
    system_info.py
    topology.py
)

PEERDIR(
    contrib/python/PyYAML
)

END()
