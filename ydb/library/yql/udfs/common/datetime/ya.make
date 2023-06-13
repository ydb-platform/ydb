YQL_UDF(datetime_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    datetime_udf.cpp
)

PEERDIR(
    library/cpp/timezone_conversion
    util/draft
)

END()
