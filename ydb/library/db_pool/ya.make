LIBRARY()

SRCS(
    db_pool.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/protos
    ydb/library/db_pool/protos
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_table
)

END()

RECURSE(
    protos
)
