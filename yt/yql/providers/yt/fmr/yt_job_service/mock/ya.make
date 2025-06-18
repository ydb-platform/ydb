LIBRARY()

SRCS(
    yql_yt_job_service_mock.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/yt_job_service/interface
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
