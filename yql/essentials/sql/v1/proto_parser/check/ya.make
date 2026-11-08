YQL_LIBRARY()

SRCS(
    check_range.cpp
)

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/public/issue
    yql/essentials/sql/v1/proto_parser
)

END()

RECURSE_FOR_TESTS(ut)
