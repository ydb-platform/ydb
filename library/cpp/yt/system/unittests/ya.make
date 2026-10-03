GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    atomic_object_ut.cpp
    copyable_atomic_ut.cpp
    count_down_latch_ut.cpp
    env_ut.cpp
    recursive_spin_lock_ut.cpp
    rw_spin_lock_ut.cpp
    spin_lock_count_ut.cpp
    spin_wait_ut.cpp
)

IF (OS_LINUX)
    SRCS(
        cpu_id_ut.cpp
    )
ENDIF()

IF (NOT OS_WINDOWS)
    SRC(spin_lock_fork_ut.cpp)
ENDIF()

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/misc
    library/cpp/yt/string
    library/cpp/yt/system
    library/cpp/testing/gtest
)

END()
