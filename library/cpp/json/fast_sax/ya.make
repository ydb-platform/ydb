LIBRARY()

# gn: move into parent

PEERDIR(
    library/cpp/json/common
)

SRCS(
    parser.rl6
    unescape.cpp
)

END()
