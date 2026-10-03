LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    at_fork.cpp
    count_down_latch.cpp
    cpu_id.cpp
    env.cpp
    event_count.cpp
    exit.cpp
    fork_aware_rw_spin_lock.cpp
    fork_aware_spin_lock.cpp
    futex.cpp
    notification_handle.cpp
    process_id.cpp
    recursive_spin_lock.cpp
    rw_spin_lock.cpp
    spin_lock.cpp
    spin_lock_count.cpp
    spin_wait.cpp
    spin_wait_hook.cpp
    thread_id.cpp
    thread_name.cpp
    writer_starving_rw_spin_lock.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/cpu_clock
    library/cpp/yt/exception
    library/cpp/yt/misc
)

IF (OS_LINUX)
    PEERDIR(
        library/cpp/yt/rseq
    )
ENDIF()

END()

RECURSE(
    benchmarks
)

RECURSE_FOR_TESTS(
    unittests
)
