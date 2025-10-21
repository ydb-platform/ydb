LIBRARY()

SRCS(
    yt_logger.cpp
    yt_logger.h
)

PEERDIR(
    yt/yql/providers/yt/lib/init_yt_api
    yt/cpp/mapreduce/interface/logging
    yql/essentials/utils/log
    yql/essentials/utils/backtrace
    library/cpp/malloc/api
)

END()
