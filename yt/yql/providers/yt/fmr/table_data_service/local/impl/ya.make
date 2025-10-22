LIBRARY()

SRCS(
    yql_yt_table_data_service_local.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/table_data_service/local/interface
    yt/yql/providers/yt/fmr/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
