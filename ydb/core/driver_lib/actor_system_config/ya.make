LIBRARY()

SRCS(
    auto_config_initializer.cpp
    auto_config_initializer.h
    config_helpers.cpp
    config_helpers.h
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/util
    ydb/library/yverify_stream
)

END()

RECURSE_FOR_TESTS(
    ut
)
