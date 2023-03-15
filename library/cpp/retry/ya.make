LIBRARY()

SRCS(
    retry.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/retry/protos
)

END()

RECURSE(
    protos
    ut
)
