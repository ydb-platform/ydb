PY23_LIBRARY()

OWNER(g:frontend-build-platform)

PY_SRCS(
    __init__.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    build/plugins/lib/nots/package_manager/pnpm
)

END()
