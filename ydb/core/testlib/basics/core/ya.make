LIBRARY()

SRCS(
    runtime.cpp
    setup.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/subsystem/interface
    ydb/core/testlib/actors
    ydb/library/actors/dnsresolver
    ydb/library/actors/interconnect
)

END()

RECURSE_FOR_TESTS(ut)
