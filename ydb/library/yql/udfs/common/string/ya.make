YQL_UDF(string_udf)

YQL_ABI_VERSION(
    2
    33
    0
)

SRCS(
    string_udf.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/arrow
    library/cpp/charset
    library/cpp/deprecated/split
    library/cpp/html/pcdata
    library/cpp/string_utils/base32
    library/cpp/string_utils/base64
    library/cpp/string_utils/levenshtein_diff
    library/cpp/string_utils/quote
)

END()
