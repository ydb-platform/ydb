YQL_UDF(protobuf_udf)

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
    yql/library/protobuf_udf
)

END()

RECURSE_FOR_TESTS(
    test
)
