LIBRARY()

SRCS(
    sanitize_label.cpp
    status_code_counters.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    yql/essentials/public/issue
    ydb/library/yql/dq/actors/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
