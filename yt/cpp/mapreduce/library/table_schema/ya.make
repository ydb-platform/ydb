LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    protobuf.h
    protobuf.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
)

END()
