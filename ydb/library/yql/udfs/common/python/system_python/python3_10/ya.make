YQL_PYTHON3_UDF(systempython3_10_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_10
    RESOURCE_NAME SystemPython3_10
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.10")
ENDIF()

PEERDIR(
    ydb/library/yql/public/udf
)

END()
