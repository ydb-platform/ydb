LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/zstd
    library/cpp/blockcodecs/core 
)

SRCS(
    GLOBAL zstd.cpp
)

END()
