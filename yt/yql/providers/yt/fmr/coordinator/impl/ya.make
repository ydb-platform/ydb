LIBRARY()

SRCS(
    yql_yt_coordinator_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/coordinator/interface
    yql/essentials/utils/log
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
