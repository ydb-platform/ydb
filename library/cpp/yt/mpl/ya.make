LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
)

PEERDIR(
    library/cpp/yt/assert
)

CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY ALL
    build
    contrib
    library
    util
)

END()

RECURSE_FOR_TESTS(
    unittests
)
