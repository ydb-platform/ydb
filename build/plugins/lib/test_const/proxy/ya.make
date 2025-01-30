SUBSCRIBER(g:ymake)

PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py=lib.test_const
)

PEERDIR(
    build/plugins/lib/test_const
)

END()
