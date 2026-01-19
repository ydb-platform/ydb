LIBRARY()


SRCS(
    yql_yt_binary_yson_comparator.cpp
    yql_yt_binary_yson_compare_impl.cpp
)
 
PEERDIR(
    library/cpp/yson
    library/cpp/yt/yson
    yql/essentials/utils
)
 
 YQL_LAST_ABI_VERSION()
END()

