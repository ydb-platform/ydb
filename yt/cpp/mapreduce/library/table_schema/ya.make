LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    arrow.cpp
    protobuf.h
    protobuf.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface

    contrib/libs/apache/arrow
)

END()
