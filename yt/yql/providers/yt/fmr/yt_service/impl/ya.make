LIBRARY()

SRCS(
    yql_yt_yt_service_impl.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/yql/providers/yt/fmr/yt_service/interface
)

YQL_LAST_ABI_VERSION()

END()
