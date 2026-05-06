LIBRARY()

SRCS(
    yql_yt_vanilla_peer_tracker.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/http/simple
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/fmr/vanilla/common
    yql/essentials/utils/log
)

END()
