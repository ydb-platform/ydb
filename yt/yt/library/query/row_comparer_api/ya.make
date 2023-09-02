LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    row_comparer_generator.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()
