LIBRARY()

PEERDIR(
    contrib/libs/zstd06
    library/cpp/blockcodecs/core
)

SRCS(
    GLOBAL legacy_zstd06.cpp
)

IF (DONT_LINK_LEGACY_ZSTD06_BLOCKCODEC)
    MESSAGE(FATAL_ERROR "Library disabled with the -D DONT_LINK_LEGACY_ZSTD06_BLOCKCODEC flag")
ENDIF()

END()
