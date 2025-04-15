GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    count_down_latch_ut.cpp
    recursive_spin_lock_ut.cpp
    spin_wait_ut.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/threading
    library/cpp/testing/gtest
)

END()
