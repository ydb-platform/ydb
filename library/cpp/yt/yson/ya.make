LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    consumer.cpp
)

PEERDIR(
    library/cpp/yt/yson_string
)

END()
