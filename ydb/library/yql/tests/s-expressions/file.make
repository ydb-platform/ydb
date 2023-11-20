PY2TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    ydb/library/yql/tools/yqlrun
    ydb/library/yql/tools/astdiff
    ydb/library/yql/tests/common/test_framework/udfs_deps
    ydb/library/yql/udfs/test/test_import
    ydb/library/yql/udfs/test/simple
)

DATA(
    arcadia/ydb/library/yql/tests/s-expressions # python files
    arcadia/ydb/library/yql/mount
    arcadia/ydb/library/yql/cfg/tests
)

PEERDIR(
    library/python/testing/swag/lib
    ydb/library/yql/protos
    ydb/library/yql/tests/common/test_framework
)

TAG(ya:dump_test_env)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

NO_CHECK_IMPORTS()

REQUIREMENTS(cpu:4 ram:13)

END()

