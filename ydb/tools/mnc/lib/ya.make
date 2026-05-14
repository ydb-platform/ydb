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
    deploy_ctx.py
    exceptions.py
    parted.py
    progress.py
    structure.py
    templates.py
    term.py
    tools.py
    ydb_config.py

    draft/tools.py
    draft/term.py

    legacy_commands/__init__.py
    legacy_commands/deploy.py
    legacy_commands/init.py
    legacy_commands/service.py
)

PEERDIR(
    contrib/python/PyYAML
    ydb/tools/mnc/scheme
    contrib/python/requests
    contrib/python/aiohttp
    contrib/python/rich
    ydb/tests/library/clients
    ydb/tests/library
)

END()
