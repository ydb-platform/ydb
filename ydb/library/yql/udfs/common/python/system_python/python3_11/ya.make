YQL_PYTHON3_UDF(systempython3_11_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_11
    RESOURCE_NAME SystemPython3_11
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.11")
ENDIF()

PEERDIR(
    ydb/library/yql/public/udf
)

END()
