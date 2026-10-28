YQL_LIBRARY()

SRCS(
    yql_yt_dump_helpers.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/yql/providers/yt/common
    yt/yql/providers/yt/lib/hash
)

END()
