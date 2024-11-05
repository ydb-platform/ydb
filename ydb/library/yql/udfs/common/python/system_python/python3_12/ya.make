YQL_PYTHON3_UDF(systempython3_12_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_12
    RESOURCE_NAME SystemPython3_12
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.12")
ENDIF()

PEERDIR(
    ydb/library/yql/public/udf
)

END()
