YQL_LIBRARY()

SRCS(
    schema.cpp
    update.cpp
    schema_ttl.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/columns
    ydb/core/tx/schemeshard/olap/indexes
    ydb/core/tx/schemeshard/olap/options
    ydb/core/tx/schemeshard/common
)

END()
