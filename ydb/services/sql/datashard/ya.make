LIBRARY()

SRCS(
    select.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/tx/datashard
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/services/sql/codec
    ydb/services/sql/parse
    ydb/services/sql/plan
)

END()
