PY3_PROGRAM(mnc)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.cli.main)

    PY_SRCS(
        main.py

        commands/__init__.py
        commands/install.py
        commands/uninstall.py
        commands/disks.py
    )

    PEERDIR(
        contrib/python/protobuf
        contrib/python/PyYAML
        contrib/python/aiohttp
        contrib/python/rich
        ydb/core/protos
        ydb/apps/dstool/lib
        ydb/tools/ydbd_slice
        ydb/tests/library/clients
        ydb/tests/library
        ydb/tools/mnc/lib
        contrib/python/requests
    )

END()
