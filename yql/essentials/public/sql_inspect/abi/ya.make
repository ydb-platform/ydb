YQL_INTERNALIZED_LIBRARY(sql_inspect_abi)

PEERDIR(
    yql/essentials/public/sql_inspect/abi/impl
)

END()

RECURSE(
    interface
    protos
    serialization
)
