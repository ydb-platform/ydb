LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/snappy
    library/cpp/blockcodecs/core 
)

SRCS(
    GLOBAL snappy.cpp
)

END()
