LIBRARY()

SRCS(
    set.cpp
    quoted_pair.cpp
    unicode_set.cpp
    unicode_set_parser.cpp
    unicode_set_token.cpp
    generated/category_ranges.cpp
    unicode_set_lexer.rl6
)

GENERATE_ENUM_SERIALIZATION(unicode_set_token.h)

END()
