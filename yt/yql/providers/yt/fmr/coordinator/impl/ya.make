LIBRARY()

SRCS(
    yql_yt_coordinator_impl.cpp
    yql_yt_partitioner.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/resource
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl
    yql/essentials/utils
    yql/essentials/utils/log
)

RESOURCE(
    default_coordinator_settings.yson default_coordinator_settings.yson
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
