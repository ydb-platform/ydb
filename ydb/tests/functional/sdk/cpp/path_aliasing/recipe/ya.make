PY3_PROGRAM(path_aliasing_recipe)

PY_SRCS(__main__.py)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    ydb/public/tools/lib/cmds
    ydb/tests/library
)

END()
