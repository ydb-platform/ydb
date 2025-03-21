LIBRARY()

SRCS(
    table_data_service.cpp
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
