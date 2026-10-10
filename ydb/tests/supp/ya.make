PY3_PROGRAM(ydb_supp)

PY_SRCS(__main__.py)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()


