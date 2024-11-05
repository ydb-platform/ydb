PY3_LIBRARY()

SUBSCRIBER(g:yql)

PY_SRCS(__init__.py)

PEERDIR(
    contrib/python/prettytable
)

END()

RECURSE_FOR_TESTS(ut)
