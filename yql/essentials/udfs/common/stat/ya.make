YQL_UDF_CONTRIB(stat_udf)

IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
    INCLUDE(${ARCADIA_ROOT}/yql/resources.inc)
ENDIF()

INCLUDE(ya.make.inc)

END()

IF (NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        test
        ut
    )
ENDIF()
