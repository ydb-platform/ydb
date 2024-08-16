LIBRARY()

PEERDIR(
    ydb/core/tx/schemeshard/olap/columns
    ydb/core/tx/schemeshard/olap/bg_tasks
    ydb/core/tx/schemeshard/olap/indexes
    ydb/core/tx/schemeshard/olap/schema
    ydb/core/tx/schemeshard/olap/common
    ydb/core/tx/schemeshard/olap/operations
    ydb/core/tx/schemeshard/olap/options
    ydb/core/tx/schemeshard/olap/layout
    ydb/core/tx/schemeshard/olap/manager
    ydb/core/tx/schemeshard/olap/store
    ydb/core/tx/schemeshard/olap/table
    ydb/core/tx/schemeshard/olap/ttl
)

END()
