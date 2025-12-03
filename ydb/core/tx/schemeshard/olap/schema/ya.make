LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/columns
    ydb/core/tx/schemeshard/olap/indexes
    ydb/core/tx/schemeshard/olap/options
    ydb/core/tx/schemeshard/common
)

END()
