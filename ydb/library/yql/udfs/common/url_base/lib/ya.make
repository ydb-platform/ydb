LIBRARY()

YQL_ABI_VERSION(
    2
    37
    0
)

SRCS(
    url_base_udf.cpp
    url_parse.cpp
    url_query.cpp
)

PEERDIR(
    library/cpp/charset
    library/cpp/string_utils/quote
    library/cpp/string_utils/url
    library/cpp/tld
    library/cpp/unicode/punycode
    library/cpp/uri
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/arrow
    contrib/libs/apache/arrow
)

END()
