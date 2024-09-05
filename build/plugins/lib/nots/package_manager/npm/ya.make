SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    npm_constants.py
    npm_lockfile.py
    npm_package_manager.py
    npm_utils.py
    npm_workspace.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
)

END()
