LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    handler.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/http

    yt/yt/library/backtrace_introspector
)

END()
