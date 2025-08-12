LIBRARY()

SRCS(
    yql_yt_table_data_service_helpers.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/table_data_service/client/impl
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/table_data_service/server
)

YQL_LAST_ABI_VERSION()

END()
