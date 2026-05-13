PY3_PROGRAM(mnc_agent)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.agent.main)

    PY_SRCS(
        main.py

        api/errors.py
        api/health.py
        api/nodes.py
        api/tasks.py

        services/features.py
        services/nodes.py
        services/tasks.py
        services/database.py

        schemas/task.py
        schemas/node.py

        config.py
    )

    PEERDIR(
        contrib/python/protobuf
        contrib/python/PyYAML
        # contrib/python/pydantic/pydantic-1  # TODO: Add pydantic support
        # contrib/python/fastapi  # TODO: Add fastapi support
        # contrib/python/aiofiles  # TODO: Add aiofiles support
        # contrib/python/uvicorn  # TODO: Add uvicorn support
        # contrib/python/aiosqlite  # TODO: Add aiosqlite support
        ydb/core/protos
        ydb/apps/dstool/lib
        ydb/tools/ydbd_slice
        ydb/tests/library/clients
        ydb/tests/library
        ydb/tools/mnc/lib
        contrib/python/requests
    )

END()
