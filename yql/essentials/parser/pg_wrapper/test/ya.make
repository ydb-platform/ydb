IF (NOT OPENSOURCE)

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
    TAG(sb:ttl=2)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram:32
)

DATA(
    arcadia/contrib/ydb/docs/ru/core/postgresql/_includes/functions.md
    arcadia/yql/essentials/cfg/udf_test
    arcadia/yql/essentials/mount
)

PEERDIR(
    yql/essentials/tests/common/test_framework
)

DEPENDS(
    yql/tools/yqlrun
    yql/essentials/udfs/common/re2
)

END()

ENDIF()

