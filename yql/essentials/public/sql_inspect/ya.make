LIBRARY()

SRCS(
    symbols.cpp
)

PEERDIR(
    yql/essentials/public/langver
    yql/essentials/public/abi_impl
    yql/essentials/public/sql_inspect/abi/interface
    yql/essentials/public/sql_inspect/abi/serialization
    yql/essentials/public/sql_inspect/abi
)

END()

RECURSE(
    abi
)

RECURSE_FOR_TESTS(
    ut
)
