LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    logger.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/yson_string
)

END()

RECURSE(
    backends
    plain_text_formatter
)

RECURSE_FOR_TESTS(
    unittests
)
