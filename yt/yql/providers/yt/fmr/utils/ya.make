LIBRARY()

SRCS(
    yql_yt_client.cpp
    yql_yt_log_context.cpp
    yql_yt_parse_records.cpp
    yql_yt_table_data_service_key.cpp
    yql_yt_table_input_streams.cpp
)

PEERDIR(
    library/cpp/http/io
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/yt_job_service/interface
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/codec
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
