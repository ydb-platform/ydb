LIBRARY()

SRCS(
    yql_yt_vanilla_tds_discovery.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/fmr/table_data_service/discovery/interface
    yt/yql/providers/yt/fmr/vanilla/common
    yt/yql/providers/yt/fmr/vanilla/peer_tracker
    yql/essentials/utils/log
)

END()
