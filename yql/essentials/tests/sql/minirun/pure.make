IF (NOT OPENSOURCE)

PY3TEST()

TEST_SRCS(
    test.py
)

REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ELSE()
    REQUIREMENTS(ram:8)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
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
)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/cfg/configs.inc)

PEERDIR(
    yql/essentials/tests/common/test_framework
    library/python/testing/swag/lib
    yql/essentials/core/file_storage/proto
)

NO_CHECK_IMPORTS()

END()

ENDIF()
