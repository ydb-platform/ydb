LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

SRCS(
    import_downloader.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/actors/core
    ydb/core/tx/datashard
)

YQL_LAST_ABI_VERSION()


END()

RECURSE_FOR_TESTS(
    ut
)
