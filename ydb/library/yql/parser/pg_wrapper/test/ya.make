PY3TEST()

TEST_SRCS(
    test_doc.py
)

SIZE(MEDIUM)

DATA(
    arcadia/ydb/library/yql/parser/pg_wrapper/functions.md
    arcadia/ydb/library/yql/cfg/udf_test
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
