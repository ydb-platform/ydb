LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    logger.cpp
    yt_log.cpp
)

PEERDIR(
    library/cpp/yt/logging
)

GENERATE_ENUM_SERIALIZATION(logger.h)

END()

RECURSE_FOR_TESTS(
    ut
)
