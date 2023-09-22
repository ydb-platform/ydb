OWNER(g:ymake)

PY23_LIBRARY()

PY_SRCS(
    __init__.py=lib.test_const
)

PEERDIR(
    build/plugins/lib/test_const
)

END()
