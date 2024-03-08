GTEST(unittester-core-concurrency)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (NOT OS_WINDOWS AND NOT ARCH_AARCH64)
    ALLOCATOR(YT)
ENDIF()

PROTO_NAMESPACE(yt)

SRCS(
    async_barrier_ut.cpp
    async_rw_lock_ut.cpp
    async_stream_pipe_ut.cpp
    async_stream_ut.cpp
    async_yson_writer_ut.cpp
    coroutines_ut.cpp
    count_down_latch_ut.cpp
    delayed_executor_ut.cpp
    fair_share_action_queue_ut.cpp
    fair_share_invoker_pool_ut.cpp
    fair_share_thread_pool_ut.cpp
    fair_throttler_ut.cpp
    fls_ut.cpp
    invoker_alarm_ut.cpp
    invoker_pool_ut.cpp
    nonblocking_batcher_ut.cpp
    nonblocking_queue_ut.cpp
    periodic_ut.cpp
    profiled_fair_share_invoker_pool_ut.cpp
    propagating_storage_ut.cpp
    quantized_executor_ut.cpp
    scheduled_executor_ut.cpp
    scheduler_ut.cpp
    suspendable_action_queue_ut.cpp
    thread_affinity_ut.cpp
    thread_pool_ut.cpp
    thread_pool_poller_ut.cpp
    throughput_throttler_ut.cpp
    two_level_fair_share_thread_pool_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TEST_FILES()

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:exotic_platform)
ENDIF()

END()
