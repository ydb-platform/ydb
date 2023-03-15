LIBRARY(version)

SRCS(
    version.cpp
    version.h
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/interconnect
    library/cpp/svnversion
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
