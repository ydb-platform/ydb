LIBRARY()

SRCS(
    yt_download.cpp
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/init_yt_api
    ydb/library/yql/core/file_storage
    ydb/library/yql/utils/log
    ydb/library/yql/utils
    library/cpp/cgiparam
    library/cpp/digest/md5
)

END()
