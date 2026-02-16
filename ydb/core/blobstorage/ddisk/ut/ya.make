UNITTEST()

FORK_SUBTESTS(MODULO)
SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/ddisk
)

SRCS(
    segment_manager_ut.cpp
)

END()
