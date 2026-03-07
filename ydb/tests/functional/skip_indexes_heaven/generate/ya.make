PY3_PROGRAM(ydb-skip_indexes_heaven-dataset_generator)

PY_MAIN(ydb.tests.functional.skip_indexes_heaven.generate.main)

PY_SRCS(
    main.py
)


ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    contrib/python/pyarrow
    contrib/python/numpy
    contrib/python/PyHamcrest
    library/python/testing/yatest_common
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()