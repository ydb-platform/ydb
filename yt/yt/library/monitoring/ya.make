LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    http_integration.cpp
    monitoring_manager.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/build
    yt/yt/library/profiling
    yt/yt/library/profiling/solomon
    library/cpp/cgiparam
)

IF (OS_LINUX)
    PEERDIR(
        yt/yt/library/ytprof
        yt/yt/library/ytprof/http

        yt/yt/library/backtrace_introspector/http
    )
ENDIF()

END()
