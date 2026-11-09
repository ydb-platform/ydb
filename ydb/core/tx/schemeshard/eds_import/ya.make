LIBRARY()

SRCS(
    restore.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/public/api/protos
    yql/essentials/sql/settings
)

END()

RECURSE_FOR_TESTS(
    ut
)
