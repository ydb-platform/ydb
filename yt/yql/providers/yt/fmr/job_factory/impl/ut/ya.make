UNITTEST()

SRCS(
    yql_yt_job_factory_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/job_factory/interface
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/utils/comparator
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
