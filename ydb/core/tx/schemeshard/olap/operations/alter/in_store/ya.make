LIBRARY()

SRCS(
    object.cpp
    update.cpp
    evolution.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
    ydb/core/tx/schemeshard/olap/ttl
    ydb/core/tx/schemeshard/olap/table
    ydb/core/tx/schemeshard/olap/store
    ydb/core/tx/schemeshard/olap/schema
)

YQL_LAST_ABI_VERSION()

END()
