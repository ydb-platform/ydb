LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/zlib
    library/cpp/blockcodecs/core 
)

SRCS(
    GLOBAL zlib.cpp
)

END()
