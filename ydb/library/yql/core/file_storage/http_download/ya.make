LIBRARY()

OWNER(g:yql)

SRCS(
    http_download.cpp
    pattern_group.cpp
)

PEERDIR(
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/utils/fetch
    ydb/library/yql/utils/log
    ydb/library/yql/utils
    library/cpp/regex/pcre
    library/cpp/digest/md5
    library/cpp/http/misc
)

END()

RECURSE_FOR_TESTS(
    ut
)
