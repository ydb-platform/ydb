LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/columns
    ydb/core/tx/schemeshard/olap/indexes
)

END()
