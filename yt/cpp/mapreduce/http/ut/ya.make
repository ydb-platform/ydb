GTEST()

INCLUDE(framing_sources.make.inc)

SRCS(
    connection_pool_ut.cpp
    host_manager_ut.cpp
    ${FRAMING_SRCS}
    http_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/http
    yt/cpp/mapreduce/tests/native/test_helpers
    yt/cpp/mapreduce/tests/yt_unittest_lib
    library/cpp/testing/common
)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

END()
