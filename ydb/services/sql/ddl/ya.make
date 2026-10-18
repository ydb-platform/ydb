LIBRARY()

SRCS(
    alter_sequence.cpp
    alter_table.cpp
    create_index.cpp
    create_schema.cpp
    create_sequence.cpp
    create_table.cpp
    drop.cpp
    drop_index.cpp
    types.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/services/sql/parse
)

END()
