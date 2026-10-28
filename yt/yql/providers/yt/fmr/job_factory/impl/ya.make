YQL_LIBRARY()

SRCS(
    yql_yt_job_factory_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/threading/future
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/job_factory/interface
    yt/yql/providers/yt/fmr/request_options
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
