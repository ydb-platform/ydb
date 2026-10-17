YQL_LIBRARY()

SRCS(
    update.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain
)

END()
