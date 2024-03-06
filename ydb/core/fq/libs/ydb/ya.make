LIBRARY()

SRCS(
    schema.cpp
    util.cpp
    ydb.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/retry
    ydb/core/base
    ydb/core/fq/libs/config
    ydb/core/fq/libs/events
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_rate_limiter
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

GENERATE_ENUM_SERIALIZATION(ydb.h)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
