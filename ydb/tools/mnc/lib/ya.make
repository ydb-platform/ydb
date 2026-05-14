PY3_LIBRARY(mnc_lib)

SUBSCRIBER(
    kruall
)

PY_SRCS(
    __init__.py
    agent_client.py
    common.py
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
    legacy_commands/agent.py
    legacy_commands/check_multinode_config.py
    legacy_commands/configs.py
    legacy_commands/deploy.py
    legacy_commands/disks.py
    legacy_commands/fullcycle.py
    legacy_commands/init.py
    legacy_commands/log.py
    legacy_commands/service.py
    legacy_commands/tls.py
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
