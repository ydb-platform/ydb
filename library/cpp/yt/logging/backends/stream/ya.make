LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    stream_log_manager.cpp
)

PEERDIR(
    library/cpp/yt/logging
    library/cpp/yt/logging/plain_text_formatter
    library/cpp/yt/string
    library/cpp/yt/threading
)

END()

RECURSE_FOR_TESTS(
    unittests
)
