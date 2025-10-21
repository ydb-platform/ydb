PY3_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    contrib/python/prettytable
)

END()

RECURSE_FOR_TESTS(ut)
