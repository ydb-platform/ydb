YQL_UDF_YDB(streaming_udf)

YQL_ABI_VERSION(
    2
    27
    0
)

SRCS(
    streaming_udf.cpp
)

PEERDIR(
    library/cpp/deprecated/kmp
)

END()

RECURSE_FOR_TESTS(
    test
)
