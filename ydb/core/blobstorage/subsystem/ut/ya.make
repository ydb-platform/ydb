UNITTEST()

SRCS(subsystem_ut.cpp)

PEERDIR(
    ydb/core/blobstorage/subsystem/mock
    ydb/core/testlib/basics/core
    ydb/library/actors/testlib
)

END()
