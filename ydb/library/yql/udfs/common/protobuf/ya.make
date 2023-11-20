YQL_UDF_YDB(protobuf_udf)

YQL_ABI_VERSION(
    2
    9
    0
)

SRCS(
    protobuf_udf.cpp
)

PEERDIR(
    library/cpp/protobuf/yql
    ydb/library/yql/minikql/protobuf_udf
    ydb/library/yql/public/udf
)

END()

RECURSE_FOR_TESTS(
    test
)
