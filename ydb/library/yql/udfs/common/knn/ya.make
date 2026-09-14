YQL_UDF_YDB(knn_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    knn.cpp
)

PEERDIR(
    library/cpp/dot_product
    library/cpp/l1_distance
    library/cpp/l2_distance
    ydb/library/yql/udfs/common/knn/half_distance
)

END()

RECURSE(
    half_distance
)

RECURSE_FOR_TESTS(
    test
)
