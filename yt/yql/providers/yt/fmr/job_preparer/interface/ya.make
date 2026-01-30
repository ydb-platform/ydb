LIBRARY()

SRCS(
    yql_yt_job_preparer_interface.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/core
    yql/essentials/utils
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
