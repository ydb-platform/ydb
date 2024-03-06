LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    guid.cpp
    source_location.cpp
    thread_name.cpp
    variant.cpp
)

PEERDIR(
    library/cpp/yt/exception
    library/cpp/yt/assert
)

CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY ALL
    build
    contrib
    library
    util
    yt/yt/library/small_containers
)

END()

RECURSE_FOR_TESTS(
    unittests
)
