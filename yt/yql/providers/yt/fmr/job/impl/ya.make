LIBRARY()

SRCS(
    yql_yt_job_impl.cpp
    yql_yt_output_stream.cpp
)

PEERDIR(
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/job/interface
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
