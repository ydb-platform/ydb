LIBRARY()

OWNER(g:yql)

SRCS(
    download_stream.cpp
    download_stream.h
    file_storage.cpp
    file_storage.h
    pattern_group.cpp
    pattern_group.h
    sized_cache.cpp
    sized_cache.h
    storage.cpp
    storage.h
    url_mapper.cpp
    url_mapper.h
    url_meta.cpp
    url_meta.h
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/cache
    library/cpp/cgiparam
    library/cpp/digest/md5
    library/cpp/logger/global
    library/cpp/regex/pcre
    library/cpp/threading/future
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/utils
    ydb/library/yql/utils/fetch
    ydb/library/yql/utils/log
)

IF (NOT OPENSOURCE)
PEERDIR(
    ydb/library/yql/core/file_storage/exporter
)
ELSE()
PEERDIR(
    ydb/library/yql/core/file_storage/exporter_dummy
)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
