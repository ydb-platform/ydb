PY23_LIBRARY()

STYLE_PYTHON()

PEERDIR(
    contrib/python/six
)

PY_SRCS(__init__.py)

END()

RECURSE_FOR_TESTS(ut)
