LIBRARY()

SRCS(
    yql_yt_log_context.cpp
    yql_yt_parse_records.cpp
    yql_yt_table_data_service_key.cpp
)

PEERDIR(
    library/cpp/http/io
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/yt_service/impl
    yt/yql/providers/yt/codec
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
