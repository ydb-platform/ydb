LIBRARY()

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/json
)

SRCS(
    reader.cpp
    writer.cpp
)

END()

RECURSE(
    tests
    ut
)
