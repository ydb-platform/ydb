YQL_UDF(string_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    string_udf.cpp
)

PEERDIR(
    library/cpp/charset
    library/cpp/deprecated/split
    library/cpp/html/pcdata
    library/cpp/string_utils/base64
    library/cpp/string_utils/levenshtein_diff
    library/cpp/string_utils/quote
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
