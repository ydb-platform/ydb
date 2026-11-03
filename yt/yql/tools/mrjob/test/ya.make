IF (OS_LINUX)
    PY3TEST()

    TEST_SRCS(
        test.py
    )

    DEPENDS(
        yt/yql/tools/mrjob/impl
    )

    END()
ENDIF()
