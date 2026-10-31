UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics)

SRCDIR(ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics)

INCLUDE(${ARCADIA_ROOT}/ydb/core/nbs/cloud/storage/core/tests/recipes/small.inc)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/compat/libs/common

    library/cpp/resource
)

SRCS(
    config_ut.cpp
    hostname_ut.cpp
    request_stats_ut.cpp
    server_stats_ut.cpp
    volume_perf_ut.cpp
    volume_stats_ut.cpp
)

END()
