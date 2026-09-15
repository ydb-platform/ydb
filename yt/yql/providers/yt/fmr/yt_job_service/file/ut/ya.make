UNITTEST()

SRCS(
    yql_yt_file_yt_job_service_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/fmr/utils/comparator
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
