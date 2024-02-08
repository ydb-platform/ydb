LIBRARY()

SRCS(
    debug_info.cpp
    defs.h
    kesus_quoter_proxy.cpp
    probes.cpp
    quoter_service.cpp
    quoter_service.h
    quoter_service_impl.h
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/containers/ring_buffer
    ydb/core/base
    ydb/core/kesus/tablet
    ydb/core/tx/scheme_cache
    ydb/core/util
    ydb/library/yql/public/issue
    ydb/library/time_series_vec
)

END()

RECURSE(
    quoter_service_bandwidth_test
)

RECURSE_FOR_TESTS(
    ut
)
