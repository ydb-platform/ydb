LIBRARY()

SRCS(
    session.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/services/sql/datashard
    ydb/services/sql/ddl
    ydb/services/sql/ischema
    ydb/services/sql/parse
)

END()
