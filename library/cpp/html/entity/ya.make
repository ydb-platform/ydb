LIBRARY()


SRCS(
    htmlentity.cpp
    decoder.rl6
)

SET(
    RAGEL6_FLAGS
    -C
    -e
    -F1
)

PEERDIR(
    library/cpp/charset
)

END()
