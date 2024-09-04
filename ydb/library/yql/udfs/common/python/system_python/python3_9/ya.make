YQL_PYTHON3_UDF(systempython3_9_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_9
    RESOURCE_NAME SystemPython3_9
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.9")
ENDIF()

PEERDIR(
    ydb/library/yql/public/udf
)

END()
