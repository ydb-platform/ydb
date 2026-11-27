LIBRARY()

SRCS(
    yql_decimal.h
    yql_decimal.cpp
    yql_decimal_serialize.h
    yql_decimal_serialize.cpp
    yql_wide_int.cpp
    yql_wide_int.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
