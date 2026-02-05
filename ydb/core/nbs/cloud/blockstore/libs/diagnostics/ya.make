LIBRARY()

SRCS(
    public.cpp
    vhost_stats.cpp
    vhost_stats_test.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/service

    ydb/core/nbs/cloud/storage/core/libs/diagnostics

    util
)

END()
