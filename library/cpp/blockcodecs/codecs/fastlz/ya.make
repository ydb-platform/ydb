LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/fastlz
    library/cpp/blockcodecs/core 
)

SRCS(
    GLOBAL fastlz.cpp
)

END()
