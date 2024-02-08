OWNER(g:frontend-build-platform)

PY23_LIBRARY()

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
)
