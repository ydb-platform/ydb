LIBRARY()

PEERDIR(
    library/cpp/lwtrace
    library/cpp/monlib/encode/legacy_protobuf/protos
)

SRCS(
    drr.cpp
    probes.cpp
)

END()

RECURSE(
    ut
)
