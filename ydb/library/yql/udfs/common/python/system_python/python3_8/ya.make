YQL_PYTHON3_UDF(systempython3_8_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_8
    RESOURCE_NAME SystemPython3_8
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.8")
ENDIF()

PEERDIR(
    ydb/library/yql/public/udf
)

END()
