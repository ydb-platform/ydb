LIBRARY()

SRCS(
    yql_yt_coordinator_client.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/retry
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers
    yt/yql/providers/yt/fmr/proto
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
