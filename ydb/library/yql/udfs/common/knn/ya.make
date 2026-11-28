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
    ydb/library/vector_distance
)

IF (MODULE_TAG != "YQL_UDF_SHARED")
    SRCS(
        remote_embedder.cpp
        remote_embedder_host_policy.cpp
        remote_embedder_http.cpp
    )
    PEERDIR(
        library/cpp/json
        library/cpp/regex/pcre
        library/cpp/threading/future
        library/cpp/time_provider
        library/cpp/uri
        ydb/core/base
        ydb/core/protos
        ydb/library/actors/http
        ydb/library/actors/interconnect/poller
        ydb/library/yql/public/udf
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    test
    ut
)
