LIBRARY()

SRCS(
    yql_yt_file_service_discovery.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/table_data_service/discovery/interface
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
