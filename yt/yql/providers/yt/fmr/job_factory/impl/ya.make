LIBRARY()

SRCS(
    yql_yt_job_factory_impl.cpp
)

PEERDIR(
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/job_factory/interface
    yt/yql/providers/yt/fmr/request_options
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
