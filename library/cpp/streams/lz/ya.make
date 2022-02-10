LIBRARY()

OWNER(
    pg
    g:util
)

PEERDIR(
    contrib/libs/fastlz
    contrib/libs/lz4
    contrib/libs/minilzo
    contrib/libs/quicklz
    contrib/libs/snappy
)

SRCS(
    lz.cpp
)

END()
