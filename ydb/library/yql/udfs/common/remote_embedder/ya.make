YQL_UDF_YDB(remote_embedder_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    remote_embedder.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    library/cpp/time_provider
    library/cpp/uri
)

END()

RECURSE_FOR_TESTS(
    ut
)
