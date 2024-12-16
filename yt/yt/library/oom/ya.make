LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    oom.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/ytprof
)

END()

IF (OS_LINUX AND NOT SANITIZER_TYPE)
    RECURSE(
        unittests
    )
ENDIF()
