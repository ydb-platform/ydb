GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    count_down_latch_ut.cpp
    recursive_spin_lock_ut.cpp
    rw_spin_lock_ut.cpp
    spin_lock_count_ut.cpp
    spin_wait_ut.cpp
)

IF (NOT OS_WINDOWS)
    SRC(spin_lock_fork_ut.cpp)
ENDIF()

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/threading
    library/cpp/testing/gtest
)

END()
