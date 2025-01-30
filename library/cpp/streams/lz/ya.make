LIBRARY()

PEERDIR(
    contrib/libs/fastlz

    library/cpp/streams/lz/snappy
    library/cpp/streams/lz/lz4
)

IF(OPENSOURCE)
    CFLAGS(-DOPENSOURCE)
ELSE()
    PEERDIR(
        contrib/libs/minilzo
        contrib/libs/quicklz
    )
    SRCS(
        minilzo.cpp
        quicklz.cpp
    )
ENDIF()

SRCS(
    lz.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
