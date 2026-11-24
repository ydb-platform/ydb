YQL_UDF_YDB(knn_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    knn.cpp
    remote_embedder.cpp
    remote_embedder_host_policy.cpp
)

PEERDIR(
    library/cpp/dot_product
    library/cpp/http/simple
    library/cpp/json
    library/cpp/l1_distance
    library/cpp/l2_distance
    library/cpp/regex/pcre
    library/cpp/time_provider
    library/cpp/uri
    ydb/core/base
    ydb/core/protos
    ydb/library/vector_distance
)

END()

RECURSE_FOR_TESTS(
    test
    ut
)
