LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/lzmasdk
    library/cpp/blockcodecs/core 
)

SRCS(
    GLOBAL lzma.cpp
)

END()
