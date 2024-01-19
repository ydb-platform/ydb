PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    library/python/func
    library/python/strings
    contrib/python/six
)

END()

RECURSE_FOR_TESTS(
    ut
)
