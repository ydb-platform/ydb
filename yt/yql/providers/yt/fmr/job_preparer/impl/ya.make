LIBRARY()

SRCS(
    yql_yt_job_preparer_impl.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/job_preparer/interface
    yt/yql/providers/yt/fmr/table_data_service/client/impl
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yt/yql/providers/yt/fmr/yt_job_service/impl
    yt/yql/providers/yt/fmr/utils
    yql/essentials/core/file_storage
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
