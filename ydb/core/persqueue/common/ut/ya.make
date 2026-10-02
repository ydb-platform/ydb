UNITTEST_FOR(ydb/core/persqueue/common)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    actor_ut.cpp
    common_app_ut.cpp
    heartbeat_ut.cpp
    key_ut.cpp
    last_counter_ut.cpp
    microseconds_sliding_window_ut.cpp
    misc_ut.cpp
    partition_id_ut.cpp
    partitioning_keys_manager_ut.cpp
    percentiles_ut.cpp
    schema_change_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/testlib/basics
    ydb/core/testlib/default
)

END()
