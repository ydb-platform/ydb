UNITTEST_FOR(yql/essentials/core/file_storage)

SRCS(
    file_storage_ut.cpp
    sized_cache_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/threading/future
    library/cpp/deprecated/atomic
    yql/essentials/utils/test_http_server
)

END()
