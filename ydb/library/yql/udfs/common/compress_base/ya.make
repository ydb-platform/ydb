YQL_UDF_YDB(compress_udf)

YQL_ABI_VERSION(
    2
    23
    0
)

SRCS(
    compress_udf.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/udfs/common/compress_base/lib
)

END()

RECURSE_FOR_TESTS(
    test
)
