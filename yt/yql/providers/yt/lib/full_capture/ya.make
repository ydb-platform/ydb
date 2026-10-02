LIBRARY()

SRCS(
    yql_yt_full_capture.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/providers/common/gateway
    yql/essentials/utils/log
    yql/essentials/utils

    library/cpp/threading/future
)

END()
