PY2TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:32)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    yql/essentials/tools/astdiff
    ydb/library/yql/tools/dqrun
    yql/tools/yqlrun
    yql/essentials/tests/common/test_framework/udfs_deps
    yql/essentials/udfs/test/test_import
)
DATA(
    arcadia/ydb/library/yql/tests/sql # python files
    arcadia/yt/yql/tests/sql/suites
    arcadia/yql/essentials/mount
    arcadia/yql/essentials/cfg/tests
)
PEERDIR(
    yql/essentials/tests/common/test_framework
    library/python/testing/swag/lib
)

NO_CHECK_IMPORTS()

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
