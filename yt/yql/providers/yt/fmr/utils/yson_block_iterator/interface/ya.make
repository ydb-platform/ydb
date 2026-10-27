YQL_LIBRARY()

SRCS(
    yql_yt_yson_block_iterator.cpp
)
 
PEERDIR(
    library/cpp/yson
    library/cpp/yt/yson
    yql/essentials/utils
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/utils/comparator
)
 
END()
