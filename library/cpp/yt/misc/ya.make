LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    guid.cpp
    source_location.cpp
)

PEERDIR(
    library/cpp/yt/exception
    library/cpp/yt/assert
    library/cpp/yt/mpl
)

IF (SANITIZER_TYPE == "address" OR SANITIZER_TYPE == "leak")
    PEERDIR(
        library/cpp/sanitizer/include
    )
ENDIF()

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
