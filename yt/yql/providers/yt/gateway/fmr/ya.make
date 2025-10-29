LIBRARY()

SRCS(
    yql_yt_fmr.cpp
)

PEERDIR(
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/provider
    yql/essentials/providers/result/expr_nodes
    yql/essentials/utils/log
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/gateway/lib
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/expr_nodes
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/lib/config_clusters
    yt/yql/providers/yt/lib/schema
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
