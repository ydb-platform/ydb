LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    json_row_parser.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/json/common
    library/cpp/json/fast_sax
    ydb/library/yql/minikql
)

END()

RECURSE_FOR_TESTS(
    ut
)
