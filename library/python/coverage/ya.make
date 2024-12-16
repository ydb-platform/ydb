PY23_LIBRARY()

SUBSCRIBER(g:yatool)

PY_CONSTRUCTOR(library.python.coverage)

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/coverage
    contrib/python/six
)

END()
