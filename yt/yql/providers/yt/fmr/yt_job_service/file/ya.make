LIBRARY()

SRCS(
    yql_yt_file_yt_job_service.cpp
)

PEERDIR(
    library/cpp/yson
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/fmr/yt_job_service/interface
    yt/yql/providers/yt/lib/yson_helpers
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
