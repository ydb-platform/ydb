UNITTEST_FOR(ydb/library/yql/core/file_storage)

TAG(ya:manual)

SRCS(
    file_storage_ut.cpp
    sized_cache_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/threading/future
    library/cpp/deprecated/atomic
    ydb/library/yql/utils/test_http_server
)

END()
