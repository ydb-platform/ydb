YQL_LIBRARY()

SRCS(
    yql_yt_coordinator_service_interface.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/request_options
)

END()
