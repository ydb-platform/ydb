LIBRARY()

SRCS(
    yql_yt_fmr.cpp
)

PEERDIR(
    yql/essentials/utils/log
    yt/yql/providers/yt/expr_nodes
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
