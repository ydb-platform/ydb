LIBRARY()

PEERDIR(
    ydb/library/yql/public/issue
)

SRCS(
    tokens.cpp
    lexer.h
    hints.cpp
    hints.h
    parse_hints_impl.cpp
    parse_hints_impl.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
