PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/boto3
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/tools/fq_runner
)

DEPENDS(
    contrib/python/moto/bin
)

IF (SANITIZER_TYPE)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_stats_mode.py
)

PY_SRCS(
    conftest.py
)

END()
