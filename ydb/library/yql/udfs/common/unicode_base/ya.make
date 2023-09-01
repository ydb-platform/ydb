YQL_UDF(unicode_udf)

YQL_ABI_VERSION(
    2
    27
    0
)

SRCS(
    unicode_base.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/unicode_base/lib
)

END()

RECURSE_FOR_TESTS(
    test
)
