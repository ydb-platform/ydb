IF (NOT OPENSOURCE)

PY3TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:20)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

#FORK_TESTS()
#FORK_SUBTESTS()
#SPLIT_FACTOR(10)

DEPENDS(
    yql/essentials/tools/astdiff
    yql/essentials/tools/minirun
    yql/essentials/tests/common/test_framework/udfs_deps
    yql/essentials/udfs/test/test_import
)
DATA(
    arcadia/yql/essentials/tests/sql/minirun # python files
    arcadia/yql/essentials/tests/sql/suites
    arcadia/yql/essentials/mount
    arcadia/yql/essentials/cfg/tests
)

PEERDIR(
    yql/essentials/tests/common/test_framework
    library/python/testing/swag/lib
    yql/essentials/core/file_storage/proto
)

NO_CHECK_IMPORTS()

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

ENDIF()
