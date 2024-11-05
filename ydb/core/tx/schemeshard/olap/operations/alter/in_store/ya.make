LIBRARY()

SRCS(
    object.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/config_shards
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/resharding
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/schema
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/transfer
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/common
    ydb/core/tx/schemeshard/olap/ttl
    ydb/core/tx/schemeshard/olap/table
    ydb/core/tx/schemeshard/olap/store
    ydb/core/tx/schemeshard/olap/schema
)

YQL_LAST_ABI_VERSION()

END()
