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
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/defs
    yql/essentials/core/file_storage/download
    yql/essentials/core/file_storage/http_download
    yql/essentials/public/issue
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/utils/fetch
)

END()

RECURSE_FOR_TESTS(
    ut
)
