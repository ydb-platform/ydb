IF(OS_LINUX)
    PY3TEST()
    TEST_SRCS(test.py)

    DEPENDS(
        ydb/library/yql/tools/mrjob
    )

    END()
ENDIF()
