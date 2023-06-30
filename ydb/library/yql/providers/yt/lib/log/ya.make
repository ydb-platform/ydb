LIBRARY()

SRCS(
    yt_logger.cpp
    yt_logger.h
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/init_yt_api
    yt/cpp/mapreduce/interface/logging
    ydb/library/yql/utils/log
)

END()
