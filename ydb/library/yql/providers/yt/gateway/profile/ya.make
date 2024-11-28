LIBRARY()

SRCS(
    yql_yt_profiling.cpp
)

PEERDIR(
    yql/essentials/utils/log
    ydb/library/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
