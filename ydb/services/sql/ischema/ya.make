LIBRARY()

SRCS(
    schemata.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/services/sql/datashard
    ydb/services/sql/parse
)

END()
