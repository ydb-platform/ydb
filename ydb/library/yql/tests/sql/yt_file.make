PY2TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
    REQUIREMENTS(ram:20)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    ydb/library/yql/tools/astdiff
    ydb/library/yql/tools/yqlrun
    ydb/library/yql/tests/common/test_framework/udfs_deps
    ydb/library/yql/udfs/test/test_import
)
DATA(
    arcadia/ydb/library/yql/tests/sql # python files
    arcadia/ydb/library/yql/mount
    arcadia/ydb/library/yql/cfg/tests
)

PEERDIR(
    ydb/library/yql/tests/common/test_framework
    library/python/testing/swag/lib
    ydb/library/yql/core/file_storage/proto
)

NO_CHECK_IMPORTS()

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

