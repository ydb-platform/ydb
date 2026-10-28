LIBRARY()

SRCS(
    sql_format.cpp
)

PEERDIR(
    yql/essentials/public/sql_format/abi
    yql/essentials/public/sql_format/abi/interface
)

END()

RECURSE(
    abi
)

RECURSE_FOR_TESTS(
    ut
)
