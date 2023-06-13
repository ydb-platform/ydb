LIBRARY()

SRCS(
    counters.cpp
    probes.cpp
    test_connection.cpp
    test_data_streams.cpp
    test_monitoring.cpp
    test_object_storage.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/xml/document
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/test_connection/events
    ydb/library/yql/providers/pq/cm_client
    ydb/library/yql/providers/solomon/async_io
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
