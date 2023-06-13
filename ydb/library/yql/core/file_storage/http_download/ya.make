LIBRARY()

SRCS(
    http_download.cpp
)

PEERDIR(
    ydb/library/yql/core/file_storage/defs
    ydb/library/yql/core/file_storage/download
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/http_download/proto
    ydb/library/yql/utils/fetch
    ydb/library/yql/utils/log
    ydb/library/yql/utils
    library/cpp/digest/md5
    library/cpp/http/misc
)

END()
