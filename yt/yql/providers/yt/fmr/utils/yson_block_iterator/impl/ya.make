LIBRARY()

SRCS(
    yql_yt_yson_tds_block_iterator.cpp
    yql_yt_yson_yt_block_iterator.cpp
)
 
 PEERDIR(
     library/cpp/yson
     library/cpp/yt/yson
     library/cpp/threading/future
     yql/essentials/utils
     yt/cpp/mapreduce/interface
     yt/yql/providers/yt/fmr/request_options
     yt/yql/providers/yt/fmr/table_data_service/interface
     yt/yql/providers/yt/fmr/utils
 )
 
 YQL_LAST_ABI_VERSION()

END()
