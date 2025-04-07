LIBRARY()

SRCS(
    yql_yt_job_factory.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
