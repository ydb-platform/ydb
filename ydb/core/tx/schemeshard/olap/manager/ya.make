LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tx/schemeshard/olap/table
    ydb/core/tx/schemeshard/olap/layout
)

END()
