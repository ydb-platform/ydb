G_BENCHMARK()

SRCS(
    process.cpp
    thread.cpp
    tscp.cpp
)

IF (OS_LINUX)
    SRCS(
        cpu_id.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/yt/system
)

END()
