LIBRARY()

SRCS(
    yql_yt_fmr_partitioner.cpp
    yql_yt_ordered_partitioner.cpp
    yql_yt_sorted_partitioner.cpp
)

PEERDIR(
    library/cpp/iterator
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/utils
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
