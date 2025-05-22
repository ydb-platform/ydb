LIBRARY()

SRCS(
    yql_yt_yt_service_mock.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/yt_service/interface
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
