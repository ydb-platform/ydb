LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    exception.cpp
)

PEERDIR(
    library/cpp/yt/assert
)

END()
