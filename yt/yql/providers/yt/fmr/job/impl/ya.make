LIBRARY()

SRCS(
    yql_yt_job_impl.cpp
    yql_yt_raw_table_queue.cpp
    yql_yt_raw_table_queue_reader.cpp
    yql_yt_raw_table_queue_writer.cpp
    yql_yt_sorted_merge_reader.cpp
    yql_yt_table_data_service_reader.cpp
    yql_yt_table_data_service_base_writer.cpp
    yql_yt_table_data_service_writer.cpp
    yql_yt_table_data_service_sorted_writer.cpp
    yql_yt_table_queue_writer_with_lock.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/yql/providers/yt/fmr/job/interface
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/utils/comparator
    yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    yt/yql/providers/yt/fmr/job_launcher
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/fmr/yt_job_service/impl
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
