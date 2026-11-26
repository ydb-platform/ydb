LIBRARY()

SRCS(
    yql/essentials/public/sql_inspect/symbols.cpp
)

PEERDIR(
    yql/essentials/public/langver
    yql/essentials/public/abi_impl
    yql/essentials/public/sql_inspect/abi/interface
    yql/essentials/public/sql_inspect/abi/serialization
    yql/essentials/public/sql_inspect/abi/impl
)

END()
