LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/mon
    ydb/core/persqueue/partition_key_range
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/tx/scheme_cache
    ydb/core/util
    ydb/library/aclib
)

SRCS(
    cache.cpp
    events.cpp
    helpers.cpp
    load_test.cpp
    monitoring.cpp
    populator.cpp
    replica.cpp
    subscriber.cpp
    two_part_description.cpp
    opaque_path_description.cpp
)

GENERATE_ENUM_SERIALIZATION(subscriber.h)

END()

RECURSE_FOR_TESTS(
    ut_cache
    ut_double_indexed
    ut_monitoring
    ut_populator
    ut_replica
    ut_subscriber
)
