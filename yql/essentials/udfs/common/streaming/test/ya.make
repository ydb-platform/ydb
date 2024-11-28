IF (OS_LINUX)
YQL_UDF_TEST_CONTRIB()
    DEPENDS(
        yql/essentials/udfs/common/digest
        yql/essentials/udfs/common/string
        yql/essentials/udfs/common/streaming
    )
    TIMEOUT(300)
    SIZE(MEDIUM)

    IF (SANITIZER_TYPE == "memory")
        TAG(ya:not_autocheck) # YQL-15385
    ENDIF()
    END()

ENDIF()
