LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (NOT OS_LINUX)
    MESSAGE(FATAL_ERROR "library/cpp/yt/rseq is Linux-only")
ENDIF()

SRCS(
    per_cpu.cpp
    rseq.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
