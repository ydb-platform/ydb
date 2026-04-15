LIBRARY()

SRCS(
    yql_yt_fmr_block_iterator.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/table_data_service/interface
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/test_tools/yson
    yt/yql/providers/yt/fmr/job/impl
)

YQL_LAST_ABI_VERSION()

END()

