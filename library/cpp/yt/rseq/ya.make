LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (NOT OS_LINUX)
    MESSAGE(FATAL_ERROR "library/cpp/yt/rseq is Linux-only")
ENDIF()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/rseq/supported_arch.inc)

SRCS(
    per_cpu.cpp
    rseq.cpp
)

PEERDIR(
    library/cpp/yt/misc
    library/cpp/yt/system
)

END()

RECURSE_FOR_TESTS(
    unittests
)
