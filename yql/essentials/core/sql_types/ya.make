LIBRARY()

SRCS(
    match_recognize.h
    match_recognize.cpp
    simple_types.h
    simple_types.cpp
    yql_atom_enums.h
    yql_callable_names.h
)
GENERATE_ENUM_SERIALIZATION(match_recognize.h)

GENERATE_ENUM_SERIALIZATION(yql_atom_enums.h)

END()

RECURSE_FOR_TESTS(
    ut
)
