LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/operations/abstract
    ydb/core/tx/schemeshard/olap/operations/alter/in_store/common
    ydb/core/tx/columnshard/data_sharing/initiator/controller
    ydb/core/tx/columnshard/data_sharing/protos
)

YQL_LAST_ABI_VERSION()

END()
