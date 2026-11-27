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
    ydb/core/kqp/common/simple
    ydb/core/kqp/query_data
    ydb/core/protos
    ydb/core/resource_pools
    ydb/core/scheme
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
    yql/essentials/core/issue
    yql/essentials/public/issue
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
