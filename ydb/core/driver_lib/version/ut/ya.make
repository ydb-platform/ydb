UNITTEST_FOR(ydb/core/driver_lib/version)

SRCS(version_ut.cpp)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/interconnect
    library/cpp/svnversion
    ydb/core/protos
)

END()
