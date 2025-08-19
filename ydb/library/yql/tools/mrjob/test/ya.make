IF(OS_LINUX)
    PY3TEST()

    TAG(ya:manual)

    TEST_SRCS(test.py)

    DEPENDS(
        ydb/library/yql/tools/mrjob
    )

    END()
ENDIF()
