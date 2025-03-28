LIBRARY()

SRCS(
    yql_yt_table_data_service_local.cpp
)

PEERDIR(
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
