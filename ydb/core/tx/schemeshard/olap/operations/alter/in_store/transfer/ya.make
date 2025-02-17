LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/columnshard/data_sharing/initiator/controller
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
