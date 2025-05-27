LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/global
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/threading
    library/cpp/yt/string
    library/cpp/yt/logging # TODO(arkady-e1ppa): Consider logging error_code crashes to stderr and drop this dep.

    util
)

SRCS(
    error.cpp
    error_attributes.cpp
    error_code.cpp
    origin_attributes.cpp
    text_yson.cpp
)

END()

RECURSE_FOR_TESTS(
    unittests
)
