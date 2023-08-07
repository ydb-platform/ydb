LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    formatter.cpp
)

PEERDIR(
    library/cpp/yt/cpu_clock
    library/cpp/yt/logging
    library/cpp/yt/string
    library/cpp/yt/misc
)

END()
