LIBRARY()

SRCS(
    yql_yt_client.cpp
    yql_yt_column_group_helpers.cpp
    yql_yt_index_serialisation.cpp
    yql_yt_log_context.cpp
    yql_yt_parse_records.cpp
    yql_yt_table_data_service_key.cpp
    yql_yt_table_input_streams.cpp
    yql_yt_tvm_helpers.cpp
    yql_yt_parser_fragment_list_index.cpp
    yql_yt_sort_helper.cpp
)

PEERDIR(
    library/cpp/http/io
    library/cpp/yson_pull
    library/cpp/yt/yson
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/fmr/tvm/interface
    yt/yql/providers/yt/fmr/utils/comparator
    yt/yql/providers/yt/fmr/yt_job_service/interface
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/lib/yson_helpers
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
