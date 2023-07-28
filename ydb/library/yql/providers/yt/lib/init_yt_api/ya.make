LIBRARY()

SRCS(
    init.cpp
)

PEERDIR(
    ydb/library/yql/utils/log
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/client
    library/cpp/yson/node
)

END()
