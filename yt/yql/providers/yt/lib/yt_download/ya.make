LIBRARY()

SRCS(
    yt_download.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/init_yt_api
    yql/essentials/core/file_storage
    yql/essentials/utils/log
    yql/essentials/utils
    library/cpp/cgiparam
    library/cpp/digest/md5
)

END()
