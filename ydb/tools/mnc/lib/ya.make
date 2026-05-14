PY3_LIBRARY(mnc_lib)

SUBSCRIBER(
    kruall
)

PY_SRCS(
    __init__.py
    agent_client.py
    common.py
    configs.py
    config.py
    deploy.py
    deploy_ctx.py
    exceptions.py
    init.py
    device.py
    progress.py
    structure.py
    templates.py
    service.py
    term.py
    tools.py
    ydb_config.py

    draft/tools.py
    draft/term.py

)

PEERDIR(
    contrib/python/protobuf
    contrib/python/PyYAML
    ydb/core/protos
    ydb/tools/mnc/scheme
    contrib/python/requests
    contrib/python/aiohttp
    contrib/python/rich
    ydb/tests/library/clients
    ydb/tests/library
)

END()
