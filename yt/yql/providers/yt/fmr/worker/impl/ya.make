LIBRARY()

SRCS(
    yql_yt_worker_impl.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/resource
    library/cpp/threading/future
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/worker/interface
    yql/essentials/utils
    yql/essentials/utils/log
)

RESOURCE(
    default_worker_settings.yson default_worker_settings.yson
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_worker_impl.h)

END()

RECURSE_FOR_TESTS(
    ut
)
