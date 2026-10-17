LIBRARY()

SRCS(
    init.cpp
)

PEERDIR(
    yql/essentials/utils/log
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/client  # gn: public dep
    library/cpp/yson/node
)

END()
