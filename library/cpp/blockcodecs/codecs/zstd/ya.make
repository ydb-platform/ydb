LIBRARY()

PEERDIR(
    contrib/libs/zstd
    library/cpp/blockcodecs/core
    library/cpp/streams/zstd
)

SRCS(
    GLOBAL zstd.cpp
)

END()
