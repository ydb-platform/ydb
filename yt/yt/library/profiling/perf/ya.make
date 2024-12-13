LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(event_counter_profiler.cpp)

IF (OS_LINUX)
    SRCS(event_counter_linux.cpp)
ELSE()
    SRCS(event_counter_dummy.cpp)
ENDIF()

PEERDIR(
    yt/yt/library/profiling
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(unittests)
