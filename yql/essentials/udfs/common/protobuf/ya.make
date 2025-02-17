YQL_UDF_CONTRIB(protobuf_udf)

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
    yql/essentials/minikql/protobuf_udf
    yql/essentials/public/udf
)

END()

RECURSE_FOR_TESTS(
    test
)
