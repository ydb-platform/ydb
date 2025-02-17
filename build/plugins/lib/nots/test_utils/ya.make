SUBSCRIBER(g:frontend_build_platform)

PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    ts_utils.py
)

PEERDIR(
    build/plugins/lib/nots/package_manager
    build/plugins/lib/nots/typescript
)

END()
