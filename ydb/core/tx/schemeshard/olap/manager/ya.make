LIBRARY()

SRCS(
    manager.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tx/schemeshard/olap/table
    ydb/core/tx/schemeshard/olap/layout
)

END()
