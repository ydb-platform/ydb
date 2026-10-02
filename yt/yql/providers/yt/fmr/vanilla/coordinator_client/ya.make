LIBRARY()

SRCS(
    yql_yt_vanilla_coordinator_client.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/fmr/coordinator/client
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/vanilla/common
    yt/yql/providers/yt/fmr/vanilla/peer_tracker
    yql/essentials/utils/log
)

END()
