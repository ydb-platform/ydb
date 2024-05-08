LIBRARY()

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/operations/alter/in_store
    ydb/core/tx/schemeshard/olap/operations/alter/standalone
    ydb/core/tx/schemeshard/olap/operations/alter/operations
    ydb/core/tx/schemeshard/olap/operations/alter/protos
    ydb/core/tx/schemeshard/olap/operations/alter/table
)

YQL_LAST_ABI_VERSION()

END()
