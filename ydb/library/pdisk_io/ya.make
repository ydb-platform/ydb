LIBRARY()

GENERATE_ENUM_SERIALIZATION(aio.h)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/libaio
        contrib/libs/liburing
    )
    SRCS(
        aio_linux.cpp
        file_params_linux.cpp
    )
ELSE(OS_LINUX)
    SRCS(
        aio_mtp.cpp
    )
ENDIF(OS_LINUX)

IF (OS_DARWIN)
    SRCS(
        file_params_darwin.cpp
    )
ENDIF(OS_DARWIN)

IF (OS_WINDOWS)
    SRCS(
        file_params_win.cpp
    )
ENDIF(OS_WINDOWS)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/wilson
    library/cpp/monlib/dynamic_counters
    ydb/core/debug
    ydb/library/pdisk_io/protos
)

SRCS(
    aio.cpp
    aio.h
    aio_map.cpp
    buffers.cpp
    buffers.h
    device_type.cpp
    device_type.h
    drivedata.cpp
    drivedata.h
    sector_map.cpp
    sector_map.h
    wcache.cpp
    wcache.h
)

END()
