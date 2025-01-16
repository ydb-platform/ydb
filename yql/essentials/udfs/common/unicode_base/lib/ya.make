LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

SRCS(
    unicode_base_udf.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/string_utils/levenshtein_diff
    library/cpp/unicode/normalization
    library/cpp/unicode/set
    yql/essentials/public/udf
    yql/essentials/utils
)

END()
