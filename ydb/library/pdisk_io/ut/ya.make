UNITTEST_FOR(ydb/library/pdisk_io)

REQUIREMENTS(cpu:1)
IF (OS_LINUX)
    SRCS(
        uring_router_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    ydb/library/pdisk_io
)

END()
