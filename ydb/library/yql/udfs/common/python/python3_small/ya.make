YQL_PYTHON3_UDF(python3_udf)

REGISTER_YQL_PYTHON_UDF(
    NAME Python3
    RESOURCE_NAME Python3
)

PEERDIR(
    ydb/library/yql/public/udf
)

END()

RECURSE_FOR_TESTS(
    test
)
