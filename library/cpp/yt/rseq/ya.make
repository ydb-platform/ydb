LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (NOT OS_LINUX)
    MESSAGE(FATAL_ERROR "library/cpp/yt/rseq is Linux-only")
ENDIF()

SRCS(
    rseq.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()
