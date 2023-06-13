GTEST()
SRCS(
    matchers_ut.cpp
    ut.cpp
)

DATA(
    arcadia/library/cpp/testing/gtest/ut/golden
)

PEERDIR(
    library/cpp/testing/hook
)

END()
