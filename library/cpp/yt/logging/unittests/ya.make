GTEST(unittester-library-logging)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (NOT OS_WINDOWS)
    ALLOCATOR(YT)
ENDIF()

SRCS(
    logger_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/logging
)

END()
