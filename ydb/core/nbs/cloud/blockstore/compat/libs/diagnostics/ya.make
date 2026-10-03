LIBRARY()

SRCS(
    config.cpp
    critical_events.cpp
    hostname.cpp
    request_stats.cpp
    server_stats.cpp
    stats_helpers.cpp
    volume_perf.cpp
    volume_stats.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/config
    ydb/core/nbs/cloud/blockstore/compat/libs/common
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/compat/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/model
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos

    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/compat/libs/diagnostics
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
    ydb/core/nbs/cloud/storage/core/libs/throttling
    ydb/core/nbs/cloud/storage/core/libs/user_stats/counter

    library/cpp/deprecated/atomic
    library/cpp/digest/crc32c
    library/cpp/histogram/hdr
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/encode/spack
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    library/cpp/monlib/service/pages/tablesorter
    library/cpp/string_utils/quote
    library/cpp/threading/hot_swap
)

END()

RECURSE_FOR_TESTS(
    ut
)
