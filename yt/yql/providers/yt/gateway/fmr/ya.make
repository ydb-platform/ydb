LIBRARY()

SRCS(
    yql_yt_fmr.cpp
)

PEERDIR(
    yql/essentials/utils/log
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/gateway/lib
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/expr_nodes
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/lib/config_clusters
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
