LIBRARY()

SRCS(subsystem.cpp)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/subsystem/interface
)

END()

RECURSE(interface mock)

RECURSE_FOR_TESTS(ut)
