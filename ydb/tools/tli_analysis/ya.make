PY3_LIBRARY()

PY_SRCS(
    __init__.py
    find_tli_chain.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
