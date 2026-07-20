GTEST(unittester-library-logging)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
    logger_ut.cpp
    tagged_payload_ut.cpp
    structured_payload_ut.cpp
    static_analysis_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/error
    library/cpp/yt/logging
)

END()
