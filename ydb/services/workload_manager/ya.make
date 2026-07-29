LIBRARY()

SRCS(
    has_full_scan_matcher.cpp
    has_path_matcher.cpp
    has_stream_matcher.cpp
    query_classifier.cpp
)

PEERDIR(
    ydb/core/cms/console

    ydb/core/kqp/common
    ydb/core/kqp/query_data

    ydb/core/mind

    ydb/core/resource_pools

    ydb/library/aclib

)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    metadata_subscription
    tables
    service
)

RECURSE_FOR_TESTS(
    ut
)
