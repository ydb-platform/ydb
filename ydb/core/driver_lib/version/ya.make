LIBRARY(version)

SRCS(
    version.cpp
    version.h
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/actors/interconnect
    library/cpp/monlib/service/pages
    library/cpp/svnversion
    library/cpp/resource
    ydb/core/protos
    ydb/core/viewer/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
