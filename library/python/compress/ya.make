PY23_LIBRARY()

PEERDIR(
    library/python/codecs
    library/python/par_apply
)

PY_SRCS(
    __init__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
