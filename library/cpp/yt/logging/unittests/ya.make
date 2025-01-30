GTEST(unittester-library-logging)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    logger_ut.cpp
    static_analysis_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/logging
)

END()
