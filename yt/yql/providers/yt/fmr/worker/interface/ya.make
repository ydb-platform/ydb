LIBRARY()

SRCS(
    yql_yt_worker.cpp
)

PEERDIR(
    library/cpp/threading/future
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/job_factory/interface
)

YQL_LAST_ABI_VERSION()

END()
