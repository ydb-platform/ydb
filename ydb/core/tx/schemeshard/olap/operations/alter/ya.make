LIBRARY()

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/operations/alter/common
    ydb/core/tx/schemeshard/olap/operations/alter/in_store
    ydb/core/tx/schemeshard/olap/operations/alter/standalone
)

YQL_LAST_ABI_VERSION()

END()
