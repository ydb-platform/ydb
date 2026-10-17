YQL_LIBRARY()

SRCS(
    update.cpp  # gn: into ydb/core/tx/schemeshard
    object.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
