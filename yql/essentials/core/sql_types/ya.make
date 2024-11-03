LIBRARY()

SRCS(
    match_recognize.h
    match_recognize.cpp
    simple_types.h
    simple_types.cpp
)
GENERATE_ENUM_SERIALIZATION(match_recognize.h)

END()

RECURSE_FOR_TESTS(
    ut
)
