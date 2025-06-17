LIBRARY()

SRCS(
    yql_yt_worker_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/job_factory/interface
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
