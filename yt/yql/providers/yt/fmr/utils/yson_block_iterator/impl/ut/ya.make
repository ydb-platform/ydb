UNITTEST()

SRCS(
    yql_yt_yson_tds_block_iterator_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/test_tools/yson
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
