LIBRARY()

SRCS(
    yql_yt_yt_service_impl.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/fmr/yt_service/interface
)

YQL_LAST_ABI_VERSION()

END()
