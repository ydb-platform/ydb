YQL_PYTHON3_UDF(systempython3_14_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME SystemPython3_14
    RESOURCE_NAME SystemPython3_14
)

IF (USE_LOCAL_PYTHON)
    LDFLAGS("-lpython3.14")
ENDIF()

PEERDIR(
    yql/essentials/public/udf
)

END()
