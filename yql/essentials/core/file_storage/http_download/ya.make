LIBRARY()

SRCS(
    http_download.cpp
)

PEERDIR(
    yql/essentials/core/file_storage/defs
    yql/essentials/core/file_storage/download
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download/proto
    yql/essentials/utils/fetch
    yql/essentials/utils/log
    yql/essentials/utils
    library/cpp/digest/md5
    library/cpp/http/misc
)

END()
