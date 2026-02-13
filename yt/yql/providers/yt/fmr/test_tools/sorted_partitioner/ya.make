LIBRARY()

SRCS(
    yql_yt_sorted_partitioner_test_tools.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    yt/yql/providers/yt/fmr/yt_job_service/file
)

YQL_LAST_ABI_VERSION()

END()

