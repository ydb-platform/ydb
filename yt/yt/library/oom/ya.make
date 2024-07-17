LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    oom.cpp
    tcmalloc_memory_limit_handler.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/ytprof
    library/cpp/yt/logging
)

END()

IF (OS_LINUX AND NOT SANITIZER_TYPE)
    RECURSE(
        unittests
    )
ENDIF()
