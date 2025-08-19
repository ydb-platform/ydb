UNITTEST_FOR(ydb/library/yql/parser/lexer_common)

TAG(ya:manual)

PEERDIR(
    ydb/library/yql/sql/v1/lexer
)


SRCS(
    hints_ut.cpp
    parse_hints_ut.cpp
)

END()
