LIBRARY()

SRCS(
    yql_yt_coordinator_proto_helpers.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/proto
    yt/yql/providers/yt/fmr/request_options/proto_helpers
)

YQL_LAST_ABI_VERSION()

END()
