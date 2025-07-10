LIBRARY()

SRCS(
    yql_yt_table_data_service_client_impl.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/http/simple
    library/cpp/retry
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/table_data_service/client/proto_helpers
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/fmr/utils
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
