GTEST(unittester-containers)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

IF (AUTOCHECK)
    ENV(SKIP_PORTO_TESTS=1)
ENDIF()

IF (DISTBUILD) # TODO(prime@): this is always on
    ENV(SKIP_PORTO_TESTS=1)
ENDIF()

SRCS(
    containers_ut.cpp
    process_ut.cpp
)

IF(OS_LINUX)
    SRCS(
        porto_resource_tracker_ut.cpp
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build
    yt/yt/library/containers
)

SIZE(MEDIUM)

END()
