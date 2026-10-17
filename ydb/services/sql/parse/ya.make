LIBRARY()

SRCS(
    lexer.cpp
    parser.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
