LIBRARY()

SRCS(
    yql_yt_job_service_impl.cpp
)

PEERDIR(
    library/cpp/yt/error
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/yql/providers/yt/fmr/yt_job_service/interface
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
