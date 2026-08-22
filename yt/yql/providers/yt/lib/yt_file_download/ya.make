LIBRARY()

SRCS(
    yql_yt_file_download.cpp
    
)

PEERDIR(
    library/cpp/streams/brotli
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yql/essentials/utils
    yql/essentials/utils/log
    yt/yql/providers/yt/gateway/lib

)

YQL_LAST_ABI_VERSION()

END()
