LIBRARY()

SRCS(
    file_storage_decorator.cpp
    file_storage.cpp
    file_storage.h
    sized_cache.cpp
    sized_cache.h
    storage.cpp
    storage.h
    url_meta.cpp
    url_meta.h
)

PEERDIR(
    library/cpp/cache
    library/cpp/digest/md5
    library/cpp/logger/global
    library/cpp/threading/future
    library/cpp/protobuf/util
    library/cpp/uri
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/core/file_storage/defs
    ydb/library/yql/core/file_storage/download
    ydb/library/yql/core/file_storage/http_download
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/utils/fetch
)

END()

RECURSE_FOR_TESTS(
    ut
)
