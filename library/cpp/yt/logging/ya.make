LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    logger.cpp
    tagged_payload.cpp
    structured_payload.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/system
    library/cpp/yt/yson_string
)

END()

RECURSE(
    backends
    benchmark
    plain_text_formatter
)

RECURSE_FOR_TESTS(
    unittests
)
