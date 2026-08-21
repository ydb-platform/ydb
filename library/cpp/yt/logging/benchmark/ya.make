G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    logger_tag.cpp
    logging.cpp
)

PEERDIR(
    library/cpp/yt/logging
    library/cpp/yt/logging/plain_text_formatter
    library/cpp/yt/string
)

END()
