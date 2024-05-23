LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks
    ydb/core/tx/schemeshard/olap/bg_tasks/adapter
    ydb/core/tx/schemeshard/olap/bg_tasks/protos
    ydb/core/tx/schemeshard/olap/bg_tasks/events
    ydb/core/tx/schemeshard/olap/bg_tasks/transactions
    ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain
)

END()
