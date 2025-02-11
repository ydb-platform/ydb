UNITTEST()

SRCS(
    yql_yt_job_factory_ut.cpp
)

PEERDIR(
    library/cpp/yt/assert
    yt/yql/providers/yt/fmr/job_factory/interface
    yt/yql/providers/yt/fmr/job_factory/impl
)

YQL_LAST_ABI_VERSION()

END()
