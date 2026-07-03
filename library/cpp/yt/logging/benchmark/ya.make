G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    logger_tag.cpp
)

PEERDIR(
    library/cpp/yt/logging
    library/cpp/yt/string
)

END()
