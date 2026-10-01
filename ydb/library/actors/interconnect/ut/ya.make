UNITTEST()

FORK_SUBTESTS()
SPLIT_FACTOR(8)
REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    channel_scheduler_ut.cpp
    connection_checker_ut.cpp
    direct_session_ut.cpp
    event_holder_pool_ut.cpp
    event_output_channel_ut.cpp
    interconnect_session_pool_mapping_ut.cpp
    interconnect_ut.cpp
    large.cpp
    outgoing_stream_ut.cpp
    poller_actor_ut.cpp
    dynamic_proxy_ut.cpp
    sticking_ut.cpp
    xdc_shuffle_ut.cpp
    v2_event_serializer_ut.cpp
    v2_io_buffers_ut.cpp
    v2_serialize_window_ut.cpp
    v2_session_ut.cpp
)

# RDMA tests use host libibverbs/libnl libraries that are not built with MSan,
# so MSan cannot reliably track initialized memory across the library boundary.
IF (SANITIZER_TYPE == "memory")
    CXXFLAGS(-DINTERCONNECT_UT_DISABLE_RDMA_TESTS)
ENDIF()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/interconnect/ut/lib
    ydb/library/actors/interconnect/ut/lib/port_manager
    ydb/library/actors/interconnect/rdma/ut/utils
    ydb/library/actors/interconnect/ut/protos
    ydb/library/actors/testlib
    library/cpp/digest/md5
    library/cpp/lwtrace
    library/cpp/testing/common
    library/cpp/logger
    library/cpp/testing/unittest
)

END()
