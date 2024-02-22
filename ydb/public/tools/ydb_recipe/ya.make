PY3_PROGRAM(ydb_recipe)

SRCDIR(
    ydb/public/tools/ydb_recipe
)

PY_SRCS(__main__.py)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    ydb/public/tools/lib/cmds
)

FILES(
    start.sh
    stop.sh
)

END()
