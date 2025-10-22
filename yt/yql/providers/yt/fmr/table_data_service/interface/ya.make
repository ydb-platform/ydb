LIBRARY()

SRCS(
    yql_yt_table_data_service.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
