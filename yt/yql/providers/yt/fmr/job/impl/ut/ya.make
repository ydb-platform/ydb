UNITTEST()

SRCS(
    yql_yt_job_ut.cpp
    yql_yt_output_stream_ut.cpp
    yql_yt_raw_table_reader_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/yt_service/mock
    yt/yql/providers/yt/fmr/table_data_service/local
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
