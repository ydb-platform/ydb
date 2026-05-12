YQL_UDF_CONTRIB(compress_udf)

YQL_ABI_VERSION(
    2
    23
    0
)

SRCS(
    compress_udf.cpp
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/udfs/common/compress_base/lib
)

END()

RECURSE_FOR_TESTS(
    test
)
