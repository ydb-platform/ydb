LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    handler.cpp
)

PEERDIR(
    library/cpp/cgiparam
    yt/yt/core/http
    yt/yt/library/ytprof
    yt/yt/library/process
)

END()
