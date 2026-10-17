LIBRARY()

SRCS(
    store.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/layout
    ydb/core/protos
)

END()
