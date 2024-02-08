PROGRAM(ytprof-example)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(main.cpp)

IF (OS_LINUX)
    LDFLAGS("-Wl,--build-id=sha1")
ENDIF()

PEERDIR(
    yt/yt/library/ytprof/http
)

END()
