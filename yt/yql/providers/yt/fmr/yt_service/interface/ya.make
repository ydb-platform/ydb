LIBRARY()

SRCS(
    yql_yt_yt_service.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/request_options
)

YQL_LAST_ABI_VERSION()

END()
