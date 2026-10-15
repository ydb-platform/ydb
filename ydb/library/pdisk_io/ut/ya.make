UNITTEST_FOR(ydb/library/pdisk_io)

IF (OS_LINUX)
    SRCS(
        aio_short_io_ut.cpp
        uring_router_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    ydb/library/pdisk_io
)

END()
