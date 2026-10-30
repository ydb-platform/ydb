LIBRARY()

SRCS(
    columns.cpp
    constraints.cpp
    sequences.cpp
    schemata.cpp
    tables.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/services/sql/datashard
    ydb/services/sql/parse
)

END()
