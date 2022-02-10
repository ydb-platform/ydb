LIBRARY()

OWNER(
    vvvv
    dcherednik
    g:yql
    g:kikimr
    g:yql_ydb_core
)

SRCS(
    yql_decimal.h
    yql_decimal.cpp
    yql_decimal_serialize.h 
    yql_decimal_serialize.cpp 
)

END()

RECURSE_FOR_TESTS(
    ut
)
