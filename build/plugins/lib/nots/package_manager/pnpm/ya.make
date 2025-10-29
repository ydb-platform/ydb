SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    constants.py
    pnpm_lockfile.py
    pnpm_package_manager.py
    pnpm_workspace.py
    utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    contrib/python/PyYAML
)

END()

RECURSE_FOR_TESTS(
    tests
)
