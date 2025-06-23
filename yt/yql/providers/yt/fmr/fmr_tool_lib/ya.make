LIBRARY()

SRCS(
    yql_yt_fmr_initializer.cpp
)

PEERDIR(
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/fmr/coordinator/client
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl
    yt/yql/providers/yt/fmr/gc_service/impl
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/fmr/yt_job_service/impl
)

YQL_LAST_ABI_VERSION()

END()
