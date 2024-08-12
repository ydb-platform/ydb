LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    piecewise_linear_function.cpp
)

PEERDIR(
    library/cpp/yt/small_containers
    library/cpp/yt/string
    util
)

CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY ALL
    build
    contrib
    library
    util
    library/cpp/yt/assert
    library/cpp/yt/small_containers
)

END()

RECURSE(
    serialize
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmark
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
