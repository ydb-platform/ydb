PY2TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TAG(sb:ttl=2)
ENDIF()

DEPENDS(
    ydb/library/yql/tests/common/test_framework/udfs_deps
    ydb/library/yql/udfs/test/test_import
    ydb/tests/tools/kqprun
)

DATA(
    arcadia/ydb/library/yql/tests/sql
    arcadia/ydb/tests/fq/yt
    arcadia/ydb/tests/fq/yt/cfg
)

PEERDIR(
    ydb/library/yql/tests/common/test_framework
)

NO_CHECK_IMPORTS()

REQUIREMENTS(ram:20)

END()
