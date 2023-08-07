GTEST(unittester-library-logging)

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    stream_log_manager_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/logging/backends/stream
)

END()
