YQL_LIBRARY()

SRCS(
    object.cpp
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/schema
    ydb/core/tx/schemeshard/olap/ttl
    ydb/core/tx/schemeshard/olap/table
)

END()
