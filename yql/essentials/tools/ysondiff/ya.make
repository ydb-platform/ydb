PY3_PROGRAM(ysondiff)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yql/essentials/tools/ysondiff/lib
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
