LIBRARY()

SRCS(
    multiblob.cpp
    multiblob_builder.cpp
)

PEERDIR(
    library/cpp/on_disk/chunks
    util/draft
)

END()
