PY3_PROGRAM(mnc_agent)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.agent.main)

    PY_SRCS(
        __init__.py
        main.py
        config.py

        api/__init__.py
        api/errors.py
        api/health.py
        api/nodes.py
        api/tasks.py
        api/disks.py

        services/__init__.py
        services/features.py
        services/database.py
        services/tasks.py
        services/nodes.py
        services/disks.py

        schemas/__init__.py
        schemas/task.py
        schemas/node.py
        schemas/disk.py
    )

    PEERDIR(
        contrib/python/aiohttp
        contrib/python/PyYAML
        ydb/tools/mnc/lib
    )

END()
