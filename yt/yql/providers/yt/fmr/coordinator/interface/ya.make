YQL_LIBRARY()

SRCS(
    yql_yt_coordinator.cpp
)

PEERDIR(
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/request_options
)

END()
