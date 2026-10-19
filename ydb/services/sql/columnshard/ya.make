LIBRARY()

SRCS(
    select.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats/arrow
    ydb/core/kqp/compute_actor
    ydb/core/scheme
    ydb/core/tx/datashard
    ydb/core/tx/long_tx_service
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/library/formats/arrow/protos
    ydb/services/sql/codec
    ydb/services/sql/parse
)

END()
