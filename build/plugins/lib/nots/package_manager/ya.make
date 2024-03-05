OWNER(g:frontend-build-platform)

PY3_LIBRARY()

STYLE_PYTHON()


PY_SRCS(
    __init__.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager/base
    build/plugins/lib/nots/package_manager/pnpm
)

END()

RECURSE(
    base
    pnpm
)
