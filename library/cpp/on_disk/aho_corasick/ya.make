LIBRARY()

SRCS(
    reader.cpp
    writer.cpp
    helpers.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/on_disk/chunks
)

END()
