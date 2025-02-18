LIBRARY()

SRCS(
    yql_yt_worker_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/worker/interface
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
