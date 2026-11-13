GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/cpp/mapreduce/http/ut/framing_sources.make.inc)

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(${FRAMING_SRCS})

PEERDIR(
    yt/cpp/mapreduce/http
    yt/cpp/mapreduce/tests/native/test_helpers
    yt/cpp/mapreduce/tests/yt_unittest_lib
    library/cpp/testing/common
)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

END()
