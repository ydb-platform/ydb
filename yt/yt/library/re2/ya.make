LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    re2.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/re2
)

END()
