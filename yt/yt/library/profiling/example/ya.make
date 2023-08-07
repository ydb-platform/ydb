PROGRAM(profiling-example)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(main.cpp)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

PEERDIR(
    yt/yt/library/profiling/solomon
    yt/yt/library/profiling/tcmalloc
    yt/yt/library/profiling/perf
)

END()
