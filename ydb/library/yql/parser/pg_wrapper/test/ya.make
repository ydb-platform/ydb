PY3TEST()

TEST_SRCS(
    test_doc.py
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

REQUIREMENTS(
    cpu:1
    ram:32
)

DATA(
    arcadia/ydb/docs/ru/core/postgresql/_includes/functions.md
    arcadia/ydb/library/yql/cfg/udf_test
    arcadia/ydb/library/yql/mount
)

PEERDIR(
    ydb/library/yql/tests/common/test_framework
)

DEPENDS(
    ydb/library/yql/tools/yqlrun
    ydb/library/yql/udfs/common/re2
)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
