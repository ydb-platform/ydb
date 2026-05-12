LIBRARY()

SRCS(
    yql_yt_job_fmr.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/request_options/proto_helpers
    yt/yql/providers/yt/fmr/table_data_service/client/impl
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yt/yql/providers/yt/fmr/table_data_service/discovery/interface
    yt/yql/providers/yt/fmr/vanilla/tds_discovery
    yt/yql/providers/yt/fmr/vanilla/peer_tracker
    yt/yql/providers/yt/fmr/tvm/impl
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/fmr/yt_job_service/impl
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    yt/yql/providers/yt/job
)

YQL_LAST_ABI_VERSION()

END()
