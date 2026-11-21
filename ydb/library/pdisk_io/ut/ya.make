UNITTEST_FOR(ydb/library/pdisk_io)

SRCS(
    aio_completion_ut.cpp
    sector_map_ut.cpp
)

IF (OS_LINUX OR OS_DARWIN)
    SRCS(
        file_params_ut.cpp
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        uring_router_ut.cpp
    )
ENDIF(OS_LINUX)

PEERDIR(
    ydb/library/pdisk_io
)

END()
