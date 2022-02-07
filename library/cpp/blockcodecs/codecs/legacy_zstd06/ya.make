LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/libs/zstd06
    library/cpp/blockcodecs/core
)

SRCS(
    GLOBAL legacy_zstd06.cpp
)

END()
