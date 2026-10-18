LIBRARY()

SRCS(
    commit.cpp
    delete.cpp
    insert.cpp
    insert_select.cpp
    select.cpp
    update.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/tx/data_events
    ydb/core/tx/datashard
    ydb/core/tx/long_tx_service
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/services/sql/codec
    ydb/services/sql/parse
    ydb/services/sql/plan
)

END()
