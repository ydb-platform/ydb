YQL_UDF_YDB(knn)

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
)


END()

RECURSE_FOR_TESTS(
    test
)