UNITTEST()

SRCS(
    yql_yt_worker_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/worker/impl
)

YQL_LAST_ABI_VERSION()

END()
