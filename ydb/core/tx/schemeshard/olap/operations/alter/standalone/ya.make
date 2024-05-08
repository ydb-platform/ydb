LIBRARY()

SRCS(
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/operations/alter/table
    ydb/core/tx/schemeshard/olap/operations/alter/protos
    ydb/core/tx/schemeshard/olap/operations/alter/standalone/schema
    ydb/core/tx/schemeshard/olap/schema
    ydb/core/tx/schemeshard/olap/ttl
    ydb/core/tx/schemeshard/olap/table
)

YQL_LAST_ABI_VERSION()

END()
