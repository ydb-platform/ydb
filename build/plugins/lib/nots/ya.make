SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    erm_json_lite.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager
    build/plugins/lib/nots/semver
)

END()

RECURSE(
    package_manager
    semver
)
