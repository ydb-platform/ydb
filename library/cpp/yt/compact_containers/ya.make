LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/malloc
    library/cpp/yt/misc
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
