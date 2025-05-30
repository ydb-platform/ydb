PY3TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:20)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

DEPENDS(
    yql/essentials/tests/common/test_framework/udfs_deps
    yql/essentials/udfs/test/test_import
    ydb/tests/tools/kqprun
)

DATA(
    arcadia/ydb/library/yql/tests/sql
    arcadia/yt/yql/tests/sql/suites
    arcadia/ydb/tests/fq/yt
    arcadia/ydb/tests/fq/yt/cfg
)

PEERDIR(
    ydb/tests/fq/tools
    yql/essentials/tests/common/test_framework
)

NO_CHECK_IMPORTS()

END()
