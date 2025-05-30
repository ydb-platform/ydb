LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (ALLOCATOR == "B" OR ALLOCATOR == "BS" OR ALLOCATOR == "C")
    CXXFLAGS(-DBALLOC)
    PEERDIR(
        library/cpp/balloc/optional
    )
ENDIF()

SRCS(
    activity_guard.cpp
    actor_bootstrapped.cpp
    actor_coroutine.cpp
    actor_coroutine.h
    actor.cpp
    actor.h
    actorid.cpp
    actorid.h
    actorsystem.cpp
    actorsystem.h
    ask.cpp
    ask.h
    buffer.cpp
    buffer.h
    callstack.cpp
    callstack.h
    config.h
    cpu_manager.cpp
    cpu_manager.h
    defs.h
    event.cpp
    event.h
    event_load.cpp
    event_local.h
    event_pb.cpp
    event_pb.h
    events.cpp
    events.h
    events_undelivered.cpp
    executelater.h
    execution_stats.cpp
    executor_pool_base.cpp
    executor_pool_base.h
    executor_pool_basic.cpp
    executor_pool_basic.h
    executor_pool_io.cpp
    executor_pool_io.h
    executor_pool_shared.cpp
    executor_pool_shared.h
    executor_thread_ctx.cpp
    executor_thread.cpp
    executor_thread.h
    hfunc.h
    interconnect.cpp
    interconnect.h
    invoke.h
    io_dispatcher.cpp
    io_dispatcher.h
    lease.h
    log.cpp
    log.h
    log_settings.cpp
    log_settings.h
    log_buffer.cpp
    log_buffer.h
    log_metrics.h
    mailbox.cpp
    mailbox.h
    mailbox_lockfree.cpp
    mailbox_lockfree.h
    mon.cpp
    mon.h
    mon_stats.cpp
    mon_stats.h
    monotonic.cpp
    monotonic.h
    monotonic_provider.cpp
    monotonic_provider.h
    thread_context.cpp
    probes.cpp
    probes.h
    process_stats.cpp
    process_stats.h
    scheduler_actor.cpp
    scheduler_actor.h
    scheduler_basic.cpp
    scheduler_basic.h
    scheduler_cookie.cpp
    scheduler_cookie.h
    scheduler_queue.h
    servicemap.h
)

GENERATE_ENUM_SERIALIZATION(defs.h)
GENERATE_ENUM_SERIALIZATION(actor.h)
GENERATE_ENUM_SERIALIZATION(log_iface.h)

PEERDIR(
    ydb/library/actors/actor_type
    ydb/library/actors/core/harmonizer
    ydb/library/actors/memory_log
    ydb/library/actors/prof
    ydb/library/actors/protos
    ydb/library/actors/util
    ydb/library/services
    library/cpp/execprofile
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/svnversion
    library/cpp/time_provider
    library/cpp/threading/future
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

RECURSE(
    harmonizer
)

RECURSE_FOR_TESTS(
    ut
    ut_fat
)
