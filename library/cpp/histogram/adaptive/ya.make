LIBRARY()


SRCS(
    common.cpp
    adaptive_histogram.cpp
    block_histogram.cpp
    fixed_bin_histogram.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/histogram/adaptive/protos
)

END()
