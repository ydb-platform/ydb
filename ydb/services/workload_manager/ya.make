LIBRARY()

SRCS(
    has_full_scan_matcher.cpp
    has_path_matcher.cpp
    has_shared_reading_matcher.cpp
    has_stream_matcher.cpp
    query_classifier.cpp
)

PEERDIR(
    library/cpp/threading/atomic_shared_ptr
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/query_data
    ydb/core/mind
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/aclib
    ydb/library/yql/providers/pq/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    metadata_subscription
    service
    tables
)

RECURSE_FOR_TESTS(
    ut
)
