LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_YT
    )
ELSE()
    SRCS(
        export_yt.cpp
        export_yt.h
        yt_shutdown.cpp
        yt_shutdown.h
        yt_wrapper.cpp
        yt_wrapper.h
    )
    PEERDIR(
        ydb/library/actors/core
        ydb/core/base
        ydb/core/protos
        ydb/library/aclib
        ydb/library/binary_json
        yt/yt/client
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
