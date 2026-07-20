UNITTEST_FOR(yql/essentials/core/file_storage)

SIZE(MEDIUM)

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
    yql/essentials/utils/fetch/proto
)

BUILD_ONLY_IF(OS_LINUX)

DATA(
    sbr://12747367284  # pv utility for bandwidth limiting in strip operations
)

END()
