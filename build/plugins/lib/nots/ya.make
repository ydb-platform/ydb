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
    build/plugins/lib/nots/typescript
)

END()

RECURSE(
    package_manager
    semver
    typescript
    test_utils
)
