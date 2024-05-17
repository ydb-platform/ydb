LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain
)

YQL_LAST_ABI_VERSION()

END()
