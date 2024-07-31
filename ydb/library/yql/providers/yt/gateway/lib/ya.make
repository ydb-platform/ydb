LIBRARY()

SRCS(
    query_cache.cpp
    query_cache.h
    temp_files.cpp
    temp_files.h
    transaction_cache.cpp
    transaction_cache.h
    user_files.cpp
    user_files.h
    yt_helpers.cpp
    yt_helpers.h
)

PEERDIR(
    library/cpp/regex/pcre
    library/cpp/string_utils/url
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    ydb/library/yql/core/file_storage
    ydb/library/yql/public/issue
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/utils/threading
    ydb/library/yql/core/type_ann
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/lib/hash
    ydb/library/yql/providers/yt/lib/res_pull
    ydb/library/yql/providers/yt/lib/url_mapper
    ydb/library/yql/providers/yt/lib/yson_helpers
)

YQL_LAST_ABI_VERSION()

END()
