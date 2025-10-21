LIBRARY()

SRCS(
    yql_yt_gc_service_impl.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/gc_service/interface
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
