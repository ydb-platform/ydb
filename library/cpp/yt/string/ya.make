LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    enum.cpp
    guid.cpp
    string.cpp
    format_string.cpp
    format.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/exception
    library/cpp/yt/misc
    library/cpp/yt/small_containers
)

CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY ALL
    build
    contrib
    library
    util
    library/cpp/yt/assert
    library/cpp/yt/misc
    library/cpp/yt/small_containers
)

END()

RECURSE_FOR_TESTS(
    unittests
)
