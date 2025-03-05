GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SIZE(MEDIUM)

SRCS(
    error_ut.cpp
    error_code_ut.cpp
)

PEERDIR(
    library/cpp/yt/error

    library/cpp/testing/gtest
)

END()
