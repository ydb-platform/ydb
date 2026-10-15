UNITTEST_FOR(ydb/library/pdisk_io)

SRCS(
    aio_completion_ut.cpp
)

IF (OS_LINUX)
    SRCS(
        uring_router_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    ydb/library/pdisk_io
)

END()
