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

END()

RECURSE_FOR_TESTS(
    ut
)
