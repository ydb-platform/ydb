PY3_PROGRAM()
REQUIREMENTS(cpu:1)

PY_SRCS(
    __main__.py
    icv2_load.py
)

END()

RECURSE_FOR_TESTS(
    ut
)
