LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/columnshard/data_sharing/protos
)

YQL_LAST_ABI_VERSION()

END()
