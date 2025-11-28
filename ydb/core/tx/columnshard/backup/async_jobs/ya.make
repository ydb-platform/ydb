LIBRARY()

SRCS(
    import_downloader.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()


END()
