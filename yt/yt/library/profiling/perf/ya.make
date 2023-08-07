LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OS_LINUX)
    SRCS(counters.cpp)
ELSE()
    SRCS(counters_other.cpp)
ENDIF()

PEERDIR(
    yt/yt/library/profiling
    yt/yt/core
)

END()
