PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ENABLE_DATASTREAMS=true)

# INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

REQUIREMENTS(ram:48)

TEST_SRCS(
    test_bulkupserts_tpch.py
    test_insertinto_selectfrom.py
    test_insert_delete_duplicate_records.py
    test_tiering.py
)

SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/sql/lib
    contrib/python/moto/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
    contrib/python/moto
    contrib/python/boto3
    library/python/testing/recipe
    library/recipes/common
)

FORK_SUBTESTS()


END()
