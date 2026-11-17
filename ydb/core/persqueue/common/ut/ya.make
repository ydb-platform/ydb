UNITTEST_FOR(ydb/core/persqueue/common)

SRCS(
    key_ut.cpp
    microseconds_sliding_window_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
