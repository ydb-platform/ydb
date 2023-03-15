PY23_LIBRARY()

OWNER(g:frontend-build-platform)

PY_SRCS(
    __init__.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager
    build/plugins/lib/nots/semver
    build/plugins/lib/nots/typescript
)

END()
