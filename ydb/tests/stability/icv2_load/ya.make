PY3_PROGRAM()

PY_SRCS(
    __main__.py
    icv2_load.py
)

END()

RECURSE_FOR_TESTS(
    ut
)
