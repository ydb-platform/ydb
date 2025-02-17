PROGRAM(proxy-example)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(main.cpp)

PEERDIR(
    yt/yt/library/profiling/solomon
)

END()
